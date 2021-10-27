package com.github.chubbard.kafka.shell;

import org.apache.kafka.clients.admin.DeleteConsumerGroupsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.jline.builtins.Completers;
import org.jline.reader.Completer;
import org.jline.reader.ParsedLine;
import org.jline.reader.impl.completer.StringsCompleter;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.jline.builtins.Completers.TreeCompleter.node;

public class DeleteCommand extends ShellCommand {

    public DeleteCommand(App app) {
        super(app);

        help = "topic <topic_name>";
    }

    @Override
    public String getCommand() {
        return "delete";
    }

    @Override
    public Completer getCompleter() {
        return new Completers.TreeCompleter(
                node("delete",
                        node("topic",
                                node(new StringsCompleter(this::getTopics))
                        ),
                        node("group",
                                node(new StringsCompleter(this::getGroups))
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
                deleteTopic( words.get(2) );
                break;
            case "group":
                deleteGroup( words.get(2) );
                break;
            default:
                printf("Unknown type %s%n",type);
                break;
        }
    }

    private void deleteGroup(String groupId) throws ExecutionException, InterruptedException {
        DeleteConsumerGroupsResult res = getAdminClient().deleteConsumerGroups(Collections.singleton(groupId));
        res.all().get();
        printf("Consumer group %s removed.%n", groupId);
    }

    private void deleteTopic(String topic) throws ExecutionException, InterruptedException {
        DeleteTopicsResult result = getAdminClient().deleteTopics( Collections.singleton(topic) );
        result.all().get();
        printf("Topic %s deleted.%n", topic);
    }
}
