package com.github.chubbard.kafka.shell;

import org.jline.reader.Completer;
import org.jline.reader.ParsedLine;
import org.jline.reader.impl.completer.StringsCompleter;

import java.util.concurrent.ExecutionException;

public class ExitCommand extends ShellCommand {
    public ExitCommand(App app) {
        super( app );
        help = "Quits the shell.";
    }

    @Override
    public String getCommand() {
        return "exit";
    }

    @Override
    public void invoke(ParsedLine line) throws ExecutionException, InterruptedException {
        app.exit();
    }
}
