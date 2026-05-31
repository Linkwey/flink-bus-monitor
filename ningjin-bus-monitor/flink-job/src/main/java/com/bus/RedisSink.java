package com.bus;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class RedisSink extends RichSinkFunction<String> {
    private final String baseKey;
    private transient JedisPool jedisPool;

    public RedisSink(String baseKey) {
        this.baseKey = baseKey;
    }

    @Override
    public void open(Configuration parameters) {
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(2);
        poolConfig.setMaxIdle(1);
        poolConfig.setMinIdle(1);
        poolConfig.setTestOnBorrow(true);
        jedisPool = new JedisPool(poolConfig, "192.168.13.129", 6379, 3000);
    }

    @Override
    public void invoke(String value, Context ctx) {
        try (Jedis jedis = jedisPool.getResource()) {
            if (baseKey.equals("bus:alerts:list")) {
                jedis.lpush(baseKey, value);
                jedis.ltrim(baseKey, 0, 49);
            } else if (baseKey.equals("bus:latest")) {
                String[] parts = value.split("\\|", 2);
                if (parts.length == 2) {
                    jedis.set("bus:latest:" + parts[0], parts[1]);
                }
            } else if (baseKey.equals("bus:indicators:top5_lines")) {
                jedis.set(baseKey, value);
                jedis.expire(baseKey, 120);
            } else {
                jedis.set(baseKey, value);
                jedis.expire(baseKey, 120);
            }
        } catch (Exception e) {
        }
    }

    @Override
    public void close() {
        if (jedisPool != null) {
            jedisPool.close();
        }
    }
}