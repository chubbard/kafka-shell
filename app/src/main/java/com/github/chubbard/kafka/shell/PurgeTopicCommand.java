package com.github.chubbard.kafka.shell;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.jline.reader.Completer;
import org.jline.reader.ParsedLine;
import org.jline.reader.impl.completer.StringsCompleter;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class PurgeTopicCommand extends ShellCommand {

    public PurgeTopicCommand(App app) {
        super(app);

        help = String.format("<topic_name>%n%1$15sPurges all messages in a topic by setting the retention.ms to 0 and restoring it back to the previous setting.", "");
    }

    @Override
    public String getCommand() {
        return "purge";
    }

    @Override
    public Completer getCompleter() {
        return new StringsCompleter(this::getTopics);
    }

    @Override
    public void invoke(ParsedLine line) throws ExecutionException, InterruptedException {
        List<String> words = line.words();
        if( words.size() < 2 ) {
            println("Missing topic name.");
            return;
        }
        String topicName = words.get(1);

        ConfigResource topicResource = new ConfigResource( ConfigResource.Type.TOPIC, topicName );
        Map<ConfigResource, Config> configs = getConfigFor(ConfigResource.Type.TOPIC, topicName);


        AlterConfigsResult alterResult = getAdminClient().incrementalAlterConfigs(
                Map.of(topicResource,
                        Arrays.asList(
                                new AlterConfigOp(new ConfigEntry(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE), AlterConfigOp.OpType.SET),
                                new AlterConfigOp(new ConfigEntry(TopicConfig.RETENTION_MS_CONFIG, "0"), AlterConfigOp.OpType.SET),
                                new AlterConfigOp(new ConfigEntry(TopicConfig.FILE_DELETE_DELAY_MS_CONFIG, "10"), AlterConfigOp.OpType.SET )
                        )
                )
        );
        alterResult.all().get();

        Thread.sleep( 1000 );

        ConfigEntry cleanUpPolicy = configs.get( topicResource ).get( TopicConfig.CLEANUP_POLICY_CONFIG );
        ConfigEntry retentionMs = configs.get( topicResource ).get(TopicConfig.RETENTION_MS_CONFIG );
        ConfigEntry fileDeleteMs = configs.get( topicResource ).get( TopicConfig.FILE_DELETE_DELAY_MS_CONFIG );
        AlterConfigsResult revert = getAdminClient().incrementalAlterConfigs(
                Map.of( topicResource,
                        Arrays.asList(
                                new AlterConfigOp(cleanUpPolicy, AlterConfigOp.OpType.SET),
                                new AlterConfigOp(retentionMs, AlterConfigOp.OpType.SET),
                                new AlterConfigOp(fileDeleteMs, AlterConfigOp.OpType.SET)
                        ))
        );

        revert.all().get();
        println("Topic " + topicName + " purged.");
    }
}
