package com.igot.cb.pores.config;

import com.igot.cb.pores.elasticsearch.dto.SearchResult;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.StringRedisSerializer;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class SearchResultRedisConfigTest {

    @Test
    void testRedisTemplateForSearchResult() {
        // Arrange
        RedisConnectionFactory mockConnectionFactory = mock(RedisConnectionFactory.class);
        SearchResultRedisConfig config = new SearchResultRedisConfig();

        // Act
        RedisTemplate<String, SearchResult> template = config.redisTemplateForSearchResult(mockConnectionFactory);

        // Assert
        assertNotNull(template);
        assertEquals(mockConnectionFactory, template.getConnectionFactory());
        assertTrue(template.getKeySerializer() instanceof StringRedisSerializer);
    }
}
