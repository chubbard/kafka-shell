package com.github.chubbard.kafka.shell;

import org.apache.kafka.clients.admin.DeleteTopicsOptions;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.jline.reader.ParsedLine;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;

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
    public void invoke(ParsedLine line) throws ExecutionException, InterruptedException {
        List<String> words = line.words();

        String type = words.get(1);
        switch( type ) {
            case "topic":
                deleteTopic( words.get(2) );
                break;
            default:
                printf("Unknown type %s%n",type);
                break;
        }
    }

    private void deleteTopic(String topic) throws ExecutionException, InterruptedException {
        DeleteTopicsResult result = getAdminClient().deleteTopics( Collections.singleton(topic) );
        result.all().get();
        printf("Topic %s deleted.%n", topic);
    }
}
