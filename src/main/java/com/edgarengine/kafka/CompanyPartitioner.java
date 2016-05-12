package com.edgarengine.kafka;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * Created by jinchengchen on 5/12/16.
 */
public class CompanyPartitioner implements Partitioner {

    private static int toPositiveInteger(long number) {
        return (int) (number % 999) + 1;
    }

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        if (!(value instanceof CompanyIndexed)) {
            return 0;
        }

        return toPositiveInteger((int)((CompanyIndexed)value).getCIK());
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
