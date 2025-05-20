package com.igot.cb.pores.config;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.lang.reflect.Field;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class RedisConfigTest {

    private RedisConfig redisConfig;

    @BeforeEach
    void setUp() throws Exception {
        redisConfig = new RedisConfig();
        setField(redisConfig, "redisHost", "localhost");
        setField(redisConfig, "redisPort", 6379);
        setField(redisConfig, "redisDataHost", "localhost");
        setField(redisConfig, "redisDataPort", 6380);
    }

    private void setField(Object target, String fieldName, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }

    @Test
    void testJedisPool() {
        JedisPool jedisPool = redisConfig.jedisPool();
        assertNotNull(jedisPool);
    }

    @Test
    void testRedisTemplateObject() {
        RedisConnectionFactory mockConnectionFactory = mock(RedisConnectionFactory.class);

        RedisTemplate<String, Object> template = redisConfig.redisTemplateObject(mockConnectionFactory);

        assertNotNull(template);
        assertEquals(StringRedisSerializer.class, template.getKeySerializer().getClass());
        assertEquals(StringRedisSerializer.class, template.getValueSerializer().getClass());
        assertEquals(mockConnectionFactory, template.getConnectionFactory());
    }

    @Test
    void testJedisDataPopulationPool() {
        JedisPool pool = redisConfig.jedisDataPopulationPool();
        assertNotNull(pool);
    }
}
