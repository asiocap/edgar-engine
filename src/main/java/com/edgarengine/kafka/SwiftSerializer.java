package com.edgarengine.kafka;

import com.facebook.swift.codec.ThriftCodecManager;
import com.facebook.swift.codec.internal.coercion.DefaultJavaCoercions;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocolFactory;

import java.io.ByteArrayOutputStream;
import java.util.Map;

/**
 * <p>
 * Customized Kafka serializer.
 * </p>
 *
 * @author Jincheng Chen
 * @see SwiftDeserializer
 */
public class SwiftSerializer implements Serializer {
    private static final ThriftCodecManager codecManager = new ThriftCodecManager();
    private static final TProtocolFactory protocolFactory = new TCompactProtocol.Factory();

    static {
        codecManager.getCatalog().addDefaultCoercions(DefaultJavaCoercions.class);
    }

    @Override
    public void configure(Map configs, boolean isKey) {
        // nothing to do
    }

    @Override
    public byte[] serialize(String topic, Object data) {
        KafkaTopicSchema schema = KafkaTopicSchema.getSchemaByTopic(topic);
        if (!schema.pojoType.isInstance(data)) {
            throw new ClassCastException(String.format("Data type %s doesn't match the topic %s!",
                    data.getClass().getCanonicalName(), topic));
        }

        return serialize(data);
    }

    public byte[] serialize(Object data) {
        ByteArrayOutputStream oStream = new ByteArrayOutputStream();
        codecManager.write(data, oStream, protocolFactory);

        return oStream.toByteArray();
    }

    @Override
    public void close() {

    }
}
