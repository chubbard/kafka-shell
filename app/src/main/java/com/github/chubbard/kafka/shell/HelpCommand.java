package com.github.chubbard.kafka.shell;

import org.apache.kafka.clients.admin.AdminClient;
import org.jline.reader.Completer;
import org.jline.reader.ParsedLine;
import org.jline.reader.impl.completer.StringsCompleter;
import org.jline.terminal.Terminal;

import java.util.concurrent.ExecutionException;

public class HelpCommand extends ShellCommand {

    public HelpCommand(App app) {
        super(app);

        help = "Receive help on the commands.";
    }

    @Override
    public String getCommand() {
        return "help";
    }

    @Override
    public void invoke(ParsedLine line) throws ExecutionException, InterruptedException {
        println("Commands:");
        app.getCommands().forEach( c -> printf("%-15s%s%n", c.getCommand(), c.getHelp()) );
    }
}
