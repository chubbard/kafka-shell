package com.github.chubbard.kafka.shell;

import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.AlterConfigsResult;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.common.config.ConfigResource;
import org.jline.builtins.Completers;
import org.jline.reader.Completer;
import org.jline.reader.ParsedLine;
import org.jline.reader.impl.completer.EnumCompleter;
import org.jline.reader.impl.completer.StringsCompleter;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.kafka.common.config.ConfigResource.*;
import static org.apache.kafka.common.config.ConfigResource.Type.*;
import static org.jline.builtins.Completers.TreeCompleter.node;

public class ConfigureCommand extends ShellCommand {
    public ConfigureCommand(App app) {
        super( app );

        help = String.format( String.join("%n%1$15s", Arrays.asList(
                "topic <topic_name> set <config_name_1=config_value_1>...<config_name_n=config_value_n>",
                "topic <topic_name> delete <config_name_1>...<config_name_n>",
                "topic <topic_name> append <config_name_1=config_value_1>...<config_name_n=config_value_n>",
                "topic <topic_name> subtract <config_name_1>...<config_name_n>",
                "broker <broker_id> set <config_name_1=config_value_1>...<config_name_n=config_value_n>",
                "broker <broker_id> delete <config_name_1>...<config_name_n>",
                "broker <broker_id> append <config_name_1=config_value_1>...<config_name_n=config_value_n>",
                "broker <broker_id> subtract <config_name_1>...<config_name_n>"
        )), "" );
    }

    @Override
    public String getCommand() {
        return "config";
    }

    @Override
    public Completer getCompleter() {
        return new Completers.TreeCompleter(
                node("config",
                    node("topic",
                            node(new StringsCompleter( this::getTopics ),
                                    node(new EnumCompleter(AlterConfigOp.OpType.class),
                                            node(new StringsCompleter( getTopicConfigs() ))
                                    )
                            )
                    ),
                    node("broker",
                            node(new StringsCompleter(this::getBrokers),
                                    node(new EnumCompleter(AlterConfigOp.OpType.class))
                            )
                    )
                )
        );
    }

    @Override
    public void invoke(ParsedLine line) throws ExecutionException, InterruptedException {
        List<String> words = line.words();
        if( words.size() < 5) {
            println("Syntax error!  See help for layout of the command.");
            return;
        }
        Type type = valueOf(words.get(1).toUpperCase());
        String name = words.get(2);
        String operation = words.get(3);
        List<String> properties = IntStream.range(4, words.size() )
                .mapToObj(words::get)
                .collect(Collectors.toList());
        switch( type ) {
            case TOPIC:
                configureTopic(name, operation, properties);
                break;
            case BROKER:
                configureBroker( name, operation, properties );
                break;
            case BROKER_LOGGER:
                break;
            default:
                println("Unknown type");
                break;
        }
    }

    private void configureTopic(String name, String operation, List<String> configs) throws ExecutionException, InterruptedException {
        ConfigResource resource = new ConfigResource(TOPIC, name);
        applyConfigToResource(resource, operation, configs);
    }

    private void configureBroker(String name, String operation, List<String> configs) throws ExecutionException, InterruptedException {
        ConfigResource resource = new ConfigResource(BROKER, name);
        applyConfigToResource(resource, operation, configs);
    }

    private void applyConfigToResource(ConfigResource resource, String operation, List<String> configs) throws InterruptedException, ExecutionException {
        Map<ConfigResource, Collection<AlterConfigOp>> configChanges = Map.of(resource,
                configs.stream().map((config) -> {
                    String[] split = config.split("=");
                    ConfigEntry entry = new ConfigEntry(split[0], split[1]);
                    return new AlterConfigOp(entry, AlterConfigOp.OpType.valueOf(operation) );
                }).collect(Collectors.toList())
        );
        AlterConfigsResult res = getAdminClient().incrementalAlterConfigs( configChanges );
        res.all().get();
        println("Configuration change applied.");
    }
}
