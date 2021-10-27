package com.github.chubbard.kafka.shell;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jline.reader.Completer;
import org.jline.reader.ParsedLine;
import org.jline.reader.impl.completer.ArgumentCompleter;
import org.jline.reader.impl.completer.StringsCompleter;
import org.jline.terminal.Terminal;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public abstract class ShellCommand {

    private static final Logger logger = LogManager.getLogger( ShellCommand.class );

    protected App app;
    protected String help;
    protected ConfigKeys configKeys;

    public ShellCommand(App app) {
        this.app = app;
    }

    public AdminClient getAdminClient() {
        return app.getAdmin();
    }

    public Terminal getTerminal() {
        return app.getTerminal();
    }

    public Consumer<byte[],byte[]> getConsumer() {
        return app.getConsumer();
    }

    public void println( String message ) {
        app.getTerminal().writer().println( message );
    }

    public void printf( String format, Object... args) {
        app.getTerminal().writer().printf( format, args );
    }

    public abstract String getCommand();

    public String getHelp() {
        return help;
    }

    public Completer getCompleter() {
        ArgumentCompleter c = new ArgumentCompleter(new StringsCompleter( getCommand() ));
        c.setStrictCommand(true);
        return c;
    }

    public abstract void invoke(ParsedLine line) throws ExecutionException, InterruptedException;

    public Collection<String> getTopicConfigs() {
        if( configKeys == null ) configKeys = new ConfigKeys();
        return configKeys.getTopicKeys();
    }

    public Collection<String> getTopics() {
        try {
            ListTopicsResult res = getAdminClient().listTopics();
            return res.names().get();
        } catch( Exception e ) {
            logger.error("Could not retrieve topics due to ", e);
            return Collections.emptyList();
        }
    }

    public Collection<String> getGroups() {
        ListConsumerGroupsResult res = getAdminClient().listConsumerGroups();
        try {
            return res.all().get().stream().map(ConsumerGroupListing::groupId).collect(Collectors.toList());
        } catch(Exception e) {
            logger.error("Could not retrieve consumer groups due to ", e);
            return Collections.emptyList();
        }
    }

    public Collection<String> getBrokers() {
        try {
            Collection<Node> nodes = getAdminClient().describeCluster().nodes().get();
            return nodes.stream().map(Node::idString).collect(Collectors.toList());
        } catch( Exception e ) {
            logger.error("Could not retrieve brokers due to ", e);
            return Collections.emptyList();
        }
    }

    public Map<ConfigResource, Config> getConfigFor(ConfigResource.Type type, String name ) throws ExecutionException, InterruptedException {
        DescribeConfigsResult configResult = getAdminClient().describeConfigs(Collections.singleton(new ConfigResource(type, name) ) );
        return configResult.all().get();
    }

    protected Boolean hasOption(List<String> words, String option) {
        OptionalInt optionPresent = IntStream.range(2, words.size())
                .filter((i) -> words.get(i).equalsIgnoreCase(option))
                .findFirst();
        return optionPresent.isPresent();
    }

    protected Optional<String> getOption(List<String> words, String wordToFind) {
        return IntStream.range(2,words.size())
                .filter((i) -> words.get(i).equalsIgnoreCase(wordToFind))
                .filter((i) -> i + 1 < words.size())
                .mapToObj(i -> words.get(i+1) )
                .findFirst();
    }

    protected Collection<ConsumerGroupListing> getConsumerGroupListings() throws InterruptedException, ExecutionException {
        ListConsumerGroupsResult result = getAdminClient().listConsumerGroups();
        return result.all().get();
    }
}
