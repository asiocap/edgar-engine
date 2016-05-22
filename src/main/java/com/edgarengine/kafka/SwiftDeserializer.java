package com.edgarengine.kafka;

import com.facebook.swift.codec.ThriftCodecManager;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocolFactory;

import java.util.Map;

/**
 * <p>
 * Customized Kafka deserializer.
 * </p>
 *
 * @author Jincheng Chen
 * @see SwiftDeserializer
 */
public class SwiftDeserializer implements Deserializer {
    private static final ThriftCodecManager codecManager = new ThriftCodecManager();
    private static final TProtocolFactory protocolFactory = new TCompactProtocol.Factory();

    @Override
    public void configure(Map configs, boolean isKey) {
        // nothing to do
    }

    @Override
    public Object deserialize(String topic, byte[] data) {
        KafkaTopicSchema schema = KafkaTopicSchema.getSchemaByTopic(topic);

        return deserialize(schema.pojoType, data);
    }

    public Object deserialize(Class clazz, byte[] data) {
        Object instance = codecManager.read(data, clazz, protocolFactory);

        return instance;
    }

    @Override
    public void close() {

    }
}
