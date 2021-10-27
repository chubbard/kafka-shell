package com.github.chubbard.kafka.shell;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.jline.reader.*;
import org.jline.reader.impl.DefaultParser;
import org.jline.reader.impl.completer.AggregateCompleter;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;

import java.io.*;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class App {

//    private static Logger logger = LoggerFactory.getLogger( App.class );

    AdminClient admin;
    Properties properties;
    Terminal terminal;
    boolean done = false;

    List<ShellCommand> commands;

    public App() {
        commands = Arrays.asList(
                new HelpCommand( this ),
                new ListCommand(this),
                new DescribeCommand(this),
                new CreateCommand( this ),
                new ConfigureCommand( this ),
                new ConsumeTopicCommand( this ),
                new DeleteCommand( this ),
                new PurgeTopicCommand( this ),
                new ExitCommand( this )
        );
    }

    public AdminClient getAdmin() {
        return admin;
    }

    public Terminal getTerminal() {
        return terminal;
    }

    public void connect() {
        admin = AdminClient.create( properties );
        terminal.writer().println("Connected.");
    }

    public void disconnect() {
        admin.close();
    }

    public Consumer<byte[],byte[]> getConsumer() {
        if( !properties.containsKey("key.deserializer") ) properties.put("key.deserializer", ByteArrayDeserializer.class.getName() );
        if( !properties.containsKey("value.deserializer") ) properties.put("value.deserializer", ByteArrayDeserializer.class.getName() );
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        return new KafkaConsumer<>( properties );
    }

    public void loadProperties(String profile) {
        this.properties = new Properties();
        File kafkaClientDir = new File(System.getProperty("user.home"), ".kafka");
        if( !kafkaClientDir.exists() ) kafkaClientDir.mkdir();
        if( kafkaClientDir.exists() ) {
            File clientConfig = new File(kafkaClientDir, profile + ".properties");
            if( clientConfig.exists() ) {
                try (Reader reader = new FileReader( clientConfig ) ) {
                    properties.load( reader );
                } catch( IOException ioe ) {
                    terminal.writer().println( ioe.getMessage() );
                }
            }
        }
    }

    public void start() throws IOException {
        LineReader reader = buildTerminal();
        loadProperties("client");
        if( !properties.isEmpty() ) connect();

        try {
            while (!done) {
                String line = reader.readLine("> ");
                line = line.trim();

                ParsedLine pl = reader.getParser().parse(line, 0);
                if (pl.word().isEmpty()) continue;

                String verb = pl.word();
                for (ShellCommand cmd : commands) {
                    try {
                        if (cmd.getCommand().equalsIgnoreCase(verb)) {
                            cmd.invoke(pl);
                            break;
                        }
                    } catch (ExecutionException e) {
                        terminal.writer().println("Error: " + e.getMessage());
                    } catch (InterruptedException e) {
                        exit();
                    }
                }
            }
        } catch( EndOfFileException ex ) {
            exit();
        } finally {
            disconnect();
        }
    }

    private LineReader buildTerminal() throws IOException {
        TerminalBuilder builder = TerminalBuilder.builder();
        Completer completer = new AggregateCompleter(commands.stream().map(ShellCommand::getCompleter).collect(Collectors.toList()));
        Parser parser = new DefaultParser();

        terminal = builder.build();

        return LineReaderBuilder.builder()
                .terminal( terminal )
                .completer( completer )
                .parser( parser )
                .build();
    }

    public List<ShellCommand> getCommands() {
        return commands;
    }

    public void exit() {
        done = true;
    }

    public static void main(String[] args) throws IOException {
        new App().start();
    }

}
