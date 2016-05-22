package com.edgarengine.kafka;

/**
 * <p>
 * Define the mapping from topic prefix to serializable data structure,
 * so that kafka client knows how data should be de-serialized.
 * </p>
 *
 * @author Jincheng Chen
 * @see SwiftSerializer
 * @see SwiftDeserializer
 */
public enum KafkaTopicSchema {
    /**
     * Form 4 topics all start with "form4-".
     */
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
