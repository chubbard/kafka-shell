package com.github.chubbard.kafka.shell;

import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.TopicConfig;

import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class ConfigKeys {

//    public static final Logger logger = LoggerFactory

    List<String> topicKeys;
//    List<String> brokerKeys;

    public List<String> getTopicKeys() {
        if( topicKeys == null ) topicKeys = populateKeys( TopicConfig.class );
        return topicKeys;
    }

//    public List<String> getBrokerKeys() {
//        if( brokerKeys == null ) brokerKeys = populateKeys( SaslConfigs.class );
//        return brokerKeys;
//    }

    private List<String> populateKeys(Class<?> configClass) {
        return Arrays.stream(configClass.getDeclaredFields())
                .filter( f -> Modifier.isPublic(f.getModifiers()) && Modifier.isStatic(f.getModifiers()) && Modifier.isFinal(f.getModifiers()) && String.class.isAssignableFrom(f.getType()) )
                .filter( f -> f.getName().endsWith("_CONFIG") )
                .map( f -> {
                    try {
                        return (String) f.get(null);
                    } catch (IllegalAccessException e) {
                        // todo logger, this shouldn't happen given we checked for public access modifier above.
                        return "";
                    }
                } )
                .filter( s -> !s.isEmpty() )
                .collect(Collectors.toList());
    }
}
