package com.github.chubbard.kafka.shell;

import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.ConfigResource;
import org.jline.builtins.Completers;
import org.jline.reader.Completer;
import org.jline.reader.ParsedLine;
import org.jline.reader.impl.completer.NullCompleter;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.jline.builtins.Completers.TreeCompleter.node;

public class CreateCommand extends ShellCommand {

    public CreateCommand(App app) {
        super(app);

        help = "topic <topic_name>";
    }

    @Override
    public String getCommand() {
        return "create";
    }

    @Override
    public Completer getCompleter() {
        return new Completers.TreeCompleter(
                node("create",
                    node("topic",
                            node(NullCompleter.INSTANCE),
                                node("partitions", "replication" )
                    )
                )
        );
    }

    @Override
    public void invoke(ParsedLine line) throws ExecutionException, InterruptedException {
        List<String> words = line.words();

        String type = words.get(1);
        switch( type ) {
            case "topic":
                int partitions = getOption( words, "partitions" ).map(Integer::parseInt).orElse(1);
                short replicationFactor = getOption( words, "replication" ).map(Short::parseShort).orElse( (short)3 );
                createTopic( words.get(2), partitions, replicationFactor );
                break;
            default:
                printf("Unknown type %s%n",type);
                break;
        }
    }

    private void createTopic(String topicName, int partitions, short replicationFactor ) throws ExecutionException, InterruptedException {
        NewTopic topic = new NewTopic(topicName, partitions, replicationFactor);
        CreateTopicsResult res = getAdminClient().createTopics( Collections.singleton(topic) );
        res.all().get();
        printf("Topic %s was created.%n", topicName);
    }
}
