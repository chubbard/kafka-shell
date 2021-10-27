package com.github.chubbard.kafka.shell;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.ConsumerGroupState;
import org.jline.builtins.Completers;
import org.jline.reader.Completer;
import org.jline.reader.ParsedLine;

import java.util.Collection;
import java.util.Comparator;
import java.util.concurrent.ExecutionException;

import static org.jline.builtins.Completers.TreeCompleter.node;

public class ListCommand extends ShellCommand {

    public ListCommand(App app) {
        super(app);

        help = "<topics|groups|offsets>";
    }

    @Override
    public String getCommand() {
        return "list";
    }

    @Override
    public Completer getCompleter() {
        return new Completers.TreeCompleter(
                node("list",
                        node("topics"),
                        node("groups"),
                        node("offsets")
                )
        );
    }

    @Override
    public void invoke(ParsedLine line) throws ExecutionException, InterruptedException {
        switch (line.words().get(1)) {
            case "groups":
                listConsumerGroups();
                break;
            case "topics":
                listTopics();
                break;
            case "offsets":
                listOffsets();
                break;
        }
    }

    private void listConsumerGroups() throws ExecutionException, InterruptedException {
        ListConsumerGroupsResult result = getAdminClient().listConsumerGroups();
        Collection<ConsumerGroupListing> groups = result.all().get();
        printf("Consumer Groups (%,d)%n", groups.size());
        println("-----------------------");
        groups.stream()
                .sorted(Comparator.comparing(ConsumerGroupListing::groupId))
                .forEach( group -> printf("%s (%s)%n",
                        group.groupId(),
                        group.state().orElse(ConsumerGroupState.UNKNOWN) )
                );
    }

    public void listTopics() throws ExecutionException, InterruptedException {
        ListTopicsResult result = getAdminClient().listTopics();
        Collection<TopicListing> listings = result.listings().get();
        printf("Topics (%,d)%n", listings.size());
        println("-----------");
        listings.stream()
                .sorted(Comparator.comparing(TopicListing::name))
                .forEach((listing) -> println( listing.name() ) );
    }

    private void listOffsets() {
        println("TODO listOffsets");
    }

}
