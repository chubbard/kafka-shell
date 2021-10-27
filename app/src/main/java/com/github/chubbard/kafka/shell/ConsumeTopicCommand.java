package com.github.chubbard.kafka.shell;

import kafka.tools.DefaultMessageFormatter;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.MessageFormatter;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.jline.builtins.Completers;
import org.jline.reader.Completer;
import org.jline.reader.ParsedLine;
import org.jline.reader.impl.completer.StringsCompleter;
import org.jline.utils.NonBlockingReader;

import java.io.IOException;
import java.io.PrintStream;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;

import static org.jline.builtins.Completers.TreeCompleter.node;

public class ConsumeTopicCommand extends ShellCommand {

    int timeoutMs = 500;

    public ConsumeTopicCommand(App app) {
        super(app);

        help = String.format(
                String.join( "%n%1$20s",
                    "<topic>",
                    "[offset <beginning|latest|numeric>]",
                    "[partition <numeric>]",
                    "[group <group_name>]",
                    "[skipErrors]",
                    "[print.key]",
                    "[print.value]",
                    "[print.headers]",
                    "[print.timestamp]",
                    "[print.partition]",
                    "[print.offset]",
                    "[key.deserializer <classname>]",
                    "[value.deserializer <classname>]",
                    "[headers.deserializer <classname>]",
                    "[key.separator <separator>]",
                    "[headers.separator <separator>]",
                    "[null.literal <literal>]"
                ), ""
        );
    }

    @Override
    public String getCommand() {
        return "consume";
    }

    @Override
    public Completer getCompleter() {
//        return new ArgumentCompleter(
//                new StringsCompleter("consume"),
//                new StringsCompleter( this::getTopics ),
//                new Completers.OptionCompleter(Arrays.asList(
//                        new Completers.TreeCompleter(node("offset", node("beginning", "latest"))),
//                        new ArgumentCompleter(new StringsCompleter("partition"), NullCompleter.INSTANCE),
//                        new ArgumentCompleter(new StringsCompleter("group"), NullCompleter.INSTANCE),
//                        new StringsCompleter("skipErrors")
//                        new ArgumentCompleter(new StringsCompleter("key.deserializer"), NullCompleter.INSTANCE),
//                        new ArgumentCompleter(new StringsCompleter("value.deserializer"), NullCompleter.INSTANCE),
//                        new ArgumentCompleter(new StringsCompleter("header.deserializer"), NullCompleter.INSTANCE)
//                ), (String option) -> {
//                    switch(option) {
//                        case "offset":
//                            return Collections.singleton(new Completers.OptDesc("offset", "offset", "beginning or latest or numeric offset"));
//                        case "partition":
//                            return Collections.singleton(new Completers.OptDesc("partition", "partition", "Partition number."));
//                        default:
//                            return Collections.singleton(new Completers.OptDesc("todo", "todo", "????"));
//                    }
//                }, 2)
//        );
        return new Completers.TreeCompleter(
                node("consume",
                        node(new StringsCompleter( this::getTopics ),
                                node("offset", node("beginning", "latest")),
                                node("partition"),
                                node("group"),
                                node("skipErrors"),
                                node("print.key", "print.value", "print.headers", "print.timestamp", "print.offset", "partition" ),
                                node("key.deserializer", "value.deserializer", "header.deserializer"),
                                node("key.separator", "headers.separator", "null.literal" )
                        )
                )
        );
    }


    @Override
    public void invoke(ParsedLine line) throws ExecutionException, InterruptedException {
        List<String> words = line.words();
        if( words.size() < 2 ) {
            println("Missing topic to consume.");
            return;
        }
        PrintStream output = new PrintStream( getTerminal().output(), true, getTerminal().encoding() );

        String topic = words.get( 1 );
        boolean skipErrors = hasOption( words, "skipErrors");
        String offset = getOption( words, "offset" ).orElse(null);
        Integer partitionNumber = getOption( words, "partition" ).map(Integer::parseInt).orElse(1);
        TopicPartition partition = new TopicPartition(topic, partitionNumber);

        BitSet printSet = new BitSet(6);
        if( hasOption(words, "print.timestamp") ) printSet.set(0);
        if( hasOption( words, "print.key") ) printSet.set(1);
        if( hasOption(words, "print.headers") ) printSet.set(2);
        if( hasOption( words, "print.offset") ) printSet.set(3);
        if( hasOption(words, "print.partition") ) printSet.set(4);
        if( hasOption(words, "print.value") ) printSet.set(5);
        if( printSet.isEmpty() ) printSet.set(5);

        Map<String,Object> formatConfig = new HashMap<>();

        addOption( formatConfig, "print.timestamp", Boolean.toString(printSet.get(0)) );
        addOption( formatConfig,"print.key", Boolean.toString(printSet.get(1)) );
        addOption( formatConfig,"print.headers", Boolean.toString(printSet.get(2)) );
        addOption( formatConfig, "print.offset", Boolean.toString(printSet.get(3)) );
        addOption( formatConfig,"print.partition", Boolean.toString(printSet.get(4)) );
        addOption( formatConfig,"print.value", Boolean.toString(printSet.get(5)) );

        addOption( formatConfig,"key.separator", getOption(words, "key.separator" ) );
        addOption( formatConfig,"line.separator", getOption(words, "line.separator" ) );
        addOption( formatConfig,"headers.separator", getOption(words, "headers.separator" ) );
        addOption( formatConfig,"null.literal", getOption( words, "null.literal" ) );

        addOption( formatConfig,"key.deserializer", getOption(words, "key.deserializer" ) );
        addOption( formatConfig,"value.deserializer", getOption(words, "value.deserializer" ) );
        addOption( formatConfig,"headers.deserializer", getOption(words, "headers.deserializer" ) );

        MessageFormatter formatter = new DefaultMessageFormatter();
        formatter.configure( formatConfig );

        int count = 0;
        printf("Reading from topic %s - Press Q to quit%n", topic);
        getTerminal().writer().flush();
        NonBlockingReader reader = getTerminal().reader();
        try(Consumer<byte[],byte[]> consumer = getConsumer()) {

            consumer.subscribe(Collections.singletonList(topic), new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {}

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    partitions.forEach((TopicPartition tp) -> {
                        if( "beginning".equalsIgnoreCase(offset) ) {
                            consumer.seekToBeginning( Collections.singletonList(tp) );
                        } else if( "latest".equalsIgnoreCase(offset) ) {
                            consumer.seekToEnd(Collections.singletonList(tp));
                        } else if( offset != null ) {
                            consumer.seek( tp, Long.parseLong(offset) );
                        }
                    });
                }
            });

            do {
                try {
                    ConsumerRecords<byte[], byte[]> cr = consumer.poll(Duration.ofMillis(timeoutMs));
                    for (ConsumerRecord<byte[], byte[]> rec : cr.records(partition)) {
                        count++;
                        formatter.writeTo(rec, output);
                    }
                } catch( WakeupException wex ) {
                    println("Interrupted.  Closing consumer.");
                    break;
                } catch (Throwable t) {
                    if (!skipErrors) throw t;
                }
            } while (!checkError(output) && !shouldWeQuit(reader));
        } catch( IOException ioe ) {
            println("Error while reading from the console. Closing consumer.");
        } finally {
            reader.shutdown();
        }

        printf("Received %,d messages%n", count);
    }

    private void addOption(Map<String, Object> formatConfig, String key, Object value) {
        if( value != null ) {
            formatConfig.put( key, value );
        }
    }

    private boolean shouldWeQuit(NonBlockingReader reader) throws IOException {
        int v = reader.read(50);
        return ((char) v) == 'q';
    }

    private boolean checkError(PrintStream output) {
        if( output.checkError() ) {
            println("Error encountered writing to the output.  Closing the consumer.");
            return true;
        }
        return false;
    }

}
