package com.igot.cb.discussion.service.impl;

import com.igot.cb.discussion.service.RateLimitingService;
import com.igot.cb.pores.util.Constants;
import com.igot.cb.pores.util.CbServerProperties;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.util.concurrent.TimeUnit;

@Service
@Slf4j
public class RateLimitingServiceImpl implements RateLimitingService {

    @Autowired
    private RedisTemplate<String, String> redisTemplate;

    @Autowired
    private CbServerProperties cbServerProperties;

    @Override
    public boolean isRateLimitExceeded(String userId) {
        return isRateLimitExceeded(userId, "answerpost", cbServerProperties.getMaxRateAnswerPostByUser());
    }

    @Override
    public boolean isRateLimitExceeded(String userId, String featureKey, int limit) {
        try {
            String key = Constants.REDIS_KEY_PREFIX + "rate_limit_" + featureKey + "_" + userId;
            String value = redisTemplate.opsForValue().get(key);
            if (value != null) {
                int count = Integer.parseInt(value);
                log.info("RateLimitingService::isRateLimitExceeded: userId={}, featureKey={}, count={}, limit={}", userId, featureKey, count, limit);
                return count >= limit;
            }
        } catch (Exception e) {
            log.error("Error checking rate limit for userId: {} and feature: {}", userId, featureKey, e);
        }
        return false;
    }

    @Override
    public void incrementAnswerPostCount(String userId) {
        incrementCount(userId, "answerpost", cbServerProperties.getRateLimitAnswerPostTtlSeconds());
    }

    @Override
    public void incrementCount(String userId, String featureKey, long ttlSeconds) {
        try {
            String key = Constants.REDIS_KEY_PREFIX + "rate_limit_" + featureKey + "_" + userId;
            Long incremented = redisTemplate.opsForValue().increment(key);
            log.info("RateLimitingService::incrementCount: userId={}, featureKey={}, incremented={}", userId, featureKey, incremented);
            if (incremented != null && incremented == 1) {
                redisTemplate.expire(key, ttlSeconds, TimeUnit.SECONDS);
            }
        } catch (Exception e) {
            log.error("Error incrementing rate limit count for userId: {} and feature: {}", userId, featureKey, e);
        }
    }
}
