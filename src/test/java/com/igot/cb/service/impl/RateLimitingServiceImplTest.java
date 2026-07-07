package com.igot.cb.service.impl;

import com.igot.cb.discussion.service.impl.RateLimitingServiceImpl;
import com.igot.cb.pores.util.Constants;
import com.igot.cb.pores.util.CbServerProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class RateLimitingServiceImplTest {

    @InjectMocks
    private RateLimitingServiceImpl rateLimitingService;

    @Mock
    private RedisTemplate<String, String> redisTemplate;

    @Mock
    private ValueOperations<String, String> valueOperations;

    @Mock
    private CbServerProperties cbServerProperties;

    private final String userId = "user123";
    private final String redisKey = Constants.REDIS_KEY_PREFIX + "rate_limit_answerpost_" + userId;

    @BeforeEach
    void setUp() {
        ReflectionTestUtils.setField(rateLimitingService, "redisTemplate", redisTemplate);
        ReflectionTestUtils.setField(rateLimitingService, "cbServerProperties", cbServerProperties);
    }

    @Test
    void testIsRateLimitExceeded_KeyDoesNotExist() {
        // Arrange
        when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        when(valueOperations.get(redisKey)).thenReturn(null);

        // Act
        boolean result = rateLimitingService.isRateLimitExceeded(userId);

        // Assert
        assertFalse(result);
        verify(valueOperations, times(1)).get(redisKey);
    }

    @Test
    void testIsRateLimitExceeded_BelowLimit() {
        // Arrange
        when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        when(valueOperations.get(redisKey)).thenReturn("50");
        when(cbServerProperties.getMaxRateAnswerPostByUser()).thenReturn(100);

        // Act
        boolean result = rateLimitingService.isRateLimitExceeded(userId);

        // Assert
        assertFalse(result);
        verify(valueOperations, times(1)).get(redisKey);
    }

    @Test
    void testIsRateLimitExceeded_AtLimit() {
        // Arrange
        when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        when(valueOperations.get(redisKey)).thenReturn("100");
        when(cbServerProperties.getMaxRateAnswerPostByUser()).thenReturn(100);

        // Act
        boolean result = rateLimitingService.isRateLimitExceeded(userId);

        // Assert
        assertTrue(result);
    }

    @Test
    void testIsRateLimitExceeded_AboveLimit() {
        // Arrange
        when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        when(valueOperations.get(redisKey)).thenReturn("105");
        when(cbServerProperties.getMaxRateAnswerPostByUser()).thenReturn(100);

        // Act
        boolean result = rateLimitingService.isRateLimitExceeded(userId);

        // Assert
        assertTrue(result);
    }

    @Test
    void testIsRateLimitExceeded_ExceptionThrown() {
        // Arrange
        when(redisTemplate.opsForValue()).thenThrow(new RuntimeException("Redis connection error"));

        // Act
        boolean result = rateLimitingService.isRateLimitExceeded(userId);

        // Assert
        assertFalse(result);
    }

    @Test
    void testIncrementAnswerPostCount_FirstIncrementSetsTtl() {
        // Arrange
        when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        when(valueOperations.increment(redisKey)).thenReturn(1L);
        when(cbServerProperties.getRateLimitAnswerPostTtlSeconds()).thenReturn(3600L);

        // Act
        rateLimitingService.incrementAnswerPostCount(userId);

        // Assert
        verify(valueOperations, times(1)).increment(redisKey);
        verify(redisTemplate, times(1)).expire(redisKey, 3600L, TimeUnit.SECONDS);
    }

    @Test
    void testIncrementAnswerPostCount_SubsequentIncrementDoesNotSetTtl() {
        // Arrange
        when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        when(valueOperations.increment(redisKey)).thenReturn(2L);

        // Act
        rateLimitingService.incrementAnswerPostCount(userId);

        // Assert
        verify(valueOperations, times(1)).increment(redisKey);
        verify(redisTemplate, never()).expire(anyString(), anyLong(), any(TimeUnit.class));
    }

    @Test
    void testIsRateLimitExceeded_Generic_BelowLimit() {
        // Arrange
        String featureKey = "discussion_create";
        String genericKey = Constants.REDIS_KEY_PREFIX + "rate_limit_" + featureKey + "_" + userId;
        when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        when(valueOperations.get(genericKey)).thenReturn("99");

        // Act
        boolean result = rateLimitingService.isRateLimitExceeded(userId, featureKey, 100);

        // Assert
        assertFalse(result);
    }

    @Test
    void testIsRateLimitExceeded_Generic_AtLimit() {
        // Arrange
        String featureKey = "discussion_create";
        String genericKey = Constants.REDIS_KEY_PREFIX + "rate_limit_" + featureKey + "_" + userId;
        when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        when(valueOperations.get(genericKey)).thenReturn("100");

        // Act
        boolean result = rateLimitingService.isRateLimitExceeded(userId, featureKey, 100);

        // Assert
        assertTrue(result);
    }

    @Test
    void testIncrementCount_Generic_SetsTtl() {
        // Arrange
        String featureKey = "discussion_create";
        String genericKey = Constants.REDIS_KEY_PREFIX + "rate_limit_" + featureKey + "_" + userId;
        when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        when(valueOperations.increment(genericKey)).thenReturn(1L);

        // Act
        rateLimitingService.incrementCount(userId, featureKey, 3600L);

        // Assert
        verify(valueOperations, times(1)).increment(genericKey);
        verify(redisTemplate, times(1)).expire(genericKey, 3600L, TimeUnit.SECONDS);
    }
}
