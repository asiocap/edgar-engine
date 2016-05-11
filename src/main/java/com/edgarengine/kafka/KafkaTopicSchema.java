package com.edgarengine.kafka;

import com.edgarengine.Form4Object;

/**
 * Created by jinchengchen on 5/10/16.
 */
public enum KafkaTopicSchema {
    Form4("form4-", Form4Object.class),

    ;
    final String topicPrefix;
    final Class pojoType;


    KafkaTopicSchema(String topicPrefix, Class pojoType) {
        this.topicPrefix = topicPrefix;
        this.pojoType = pojoType;
    }

    public static KafkaTopicSchema getSchemaByTopic(String topic) {
        if (topic == null) {
            return null;
        }

        for (KafkaTopicSchema schema : values()) {
            if (topic.startsWith(schema.topicPrefix)) {
                return schema;
            }
        }

        return null;
    }
}
