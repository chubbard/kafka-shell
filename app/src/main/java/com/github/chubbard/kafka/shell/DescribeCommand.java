package com.github.chubbard.kafka.shell;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.config.ConfigResource;
import org.jline.builtins.Completers;
import org.jline.reader.Completer;
import org.jline.reader.ParsedLine;
import org.jline.reader.impl.completer.StringsCompleter;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.jline.builtins.Completers.TreeCompleter.node;

public class DescribeCommand extends ShellCommand {

    public DescribeCommand(App app) {
        super( app );

        help = String.format(String.join("%n%1$15s", Arrays.asList(
                "topic <topic_name> [config name]",
                "group <group_name>",
                "cluster",
                "broker <broker_id> [config name]")
        ), "");
    }

    @Override
    public String getCommand() {
        return "describe";
    }

    @Override
    public Completer getCompleter() {
        return new Completers.TreeCompleter(
            node("describe",
                    node("topic", node( new StringsCompleter(this::getTopics )) ),
                    node("groups", node( new StringsCompleter(this::getGroups )) ),
                    node("cluster"),
                    node("broker", node( new StringsCompleter(this::getBrokers )) )
            )
        );
    }

    @Override
    public void invoke(ParsedLine line) throws ExecutionException, InterruptedException {
        List<String> words = line.words();
        switch( words.get(1) ) {
            case "group":
                describeGroup( words.get(2) );
                break;
            case "topic":
                describeTopic( words.get(2), words.size() > 3 ? words.get(3) : null );
                break;
            case "cluster":
                describeCluster();
                break;
            case "broker":
                describeBroker( words.get(2), words.size() > 3 ? words.get(3) : null );
                break;
        }
    }

    private void describeBroker(String brokerId, String configName) throws ExecutionException, InterruptedException {
        Map<ConfigResource, Config> configs = getConfigFor(ConfigResource.Type.BROKER, brokerId );

        for( Map.Entry<ConfigResource,Config> config : configs.entrySet() ) {
            println( "Broker: " + config.getKey().name() );
            printConfig( config.getValue(), configName );
        }
    }

    private void describeGroup(String group) throws ExecutionException, InterruptedException {
        DescribeConsumerGroupsResult res = getAdminClient().describeConsumerGroups( Collections.singleton(group) );
        Map<String, KafkaFuture<ConsumerGroupDescription>> groups = res.describedGroups();

        for( Map.Entry<String,KafkaFuture<ConsumerGroupDescription>> e : groups.entrySet() ) {
            String key = e.getKey();
            KafkaFuture<ConsumerGroupDescription> future = e.getValue();

            ConsumerGroupDescription desc = future.get();
            printf( "Group=%s (state=%s partition assigner=%s)%n", desc.groupId(), desc.state().toString(), desc.partitionAssignor() );
            printf( "members=[%n%s]%n", desc.members()
                    .stream()
                    .map( m -> String.format( "%s@%s ==> %s", m.consumerId(),m.host(), m.groupInstanceId().orElse("No group") ) )
                    .collect( Collectors.joining("\n")));
            println("--------------------------------------------");
        }
    }

    private void describeCluster() throws ExecutionException, InterruptedException {
        DescribeClusterResult res = getAdminClient().describeCluster();
        String clusterId = res.clusterId().get();
        Set<AclOperation> op = res.authorizedOperations().get();
        Node controller = res.controller().get();
        Collection<Node> nodes = res.nodes().get();

        printf("Cluster ID: %s%n", clusterId);
        if( op != null ) printf("ACL Operations: %s%n", op.stream().map(acl -> String.format("0x%x", acl.code()) ).collect(Collectors.joining(",")));
        println("Nodes:");
        println("---------");
        for (Node node : nodes) {
            String control = controller.id() == node.id() ? "*->" : "   ";
            printf( "%s %s@host=%s:%d [rack=%s]%n", control, node.idString(), node.host(), node.port(), node.hasRack() ? node.rack() : "no rack" );
        }
        println("\n* denotes controller");
    }

    private void describeTopic(String topic, String configName) throws ExecutionException, InterruptedException {
        DescribeTopicsResult res = getAdminClient().describeTopics( Collections.singleton(topic) );
        Map<String,TopicDescription> descriptions = res.all().get();
        Map<ConfigResource, Config> configs = getConfigFor(ConfigResource.Type.TOPIC, topic);

        descriptions.values().stream()
                .sorted( Comparator.comparing(TopicDescription::name) )
                .forEach( topicDesc -> {
                    Config config = configs.get( new ConfigResource(ConfigResource.Type.TOPIC, topicDesc.name() ) );
                    // todo topicDesc.authorizedOptions()
                    printf("%s (partitions=%s)%n", topicDesc.name(), topicDesc.partitions().size() );
                    printConfig(config, configName);
                } );
    }

    private void printConfig(Config config, String configName) {
        println("Config:");
        println("----------");
        config.entries().stream()
                .filter( (e) -> configName == null || e.name().toLowerCase().startsWith(configName.toLowerCase()) )
                .sorted(Comparator.comparing(ConfigEntry::name))
                .forEach( entry -> printf("%s = %s%n", entry.name(), entry.value() ) );
    }
}
