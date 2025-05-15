package com.igot.cb.pores.cache;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.pores.util.Constants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Service
@Slf4j
public class CacheService {

  @Autowired
  private RedisTemplate<String, String> redisTemplate;

  @Autowired
  @Qualifier(Constants.REDIS_DATA_TEMPLATE)
  private RedisTemplate<String, String> redisDataTemplate;

  @Autowired
  private ObjectMapper objectMapper;

  @Value("${spring.redis.cacheTtl}")
  private long cacheTtl;

  public void putCache(String key, Object object) {
    try {
      String data = objectMapper.writeValueAsString(object);
      redisTemplate.opsForValue().set(Constants.REDIS_KEY_PREFIX + key, data, cacheTtl, TimeUnit.SECONDS);
    } catch (Exception e) {
      log.error("Error while putting data in Redis cache: {}", e.getMessage(), e);
    }
  }

  public String getCache(String key) {
    try {
      return redisTemplate.opsForValue().get(Constants.REDIS_KEY_PREFIX + key);
    } catch (Exception e) {
      log.error("Error while getting data from Redis cache: {}", e.getMessage(), e);
      return null;
    }
  }

  public Long deleteCache(String key) {
    try {
      boolean result = redisTemplate.delete(Constants.REDIS_KEY_PREFIX + key);
      if (result) {
        log.info("Field deleted successfully from key {}.", key);
      } else {
        log.warn("Field not found in key {}.", key);
      }
    } catch (Exception e) {
      log.error("Error while deleting key {} from Redis: {}", key, e.getMessage(), e);
    }
    return null;
  }

  public List<Object> hget(List<String> keys) {
    List<Object> resultList = new ArrayList<>();
    try {
      for (String key : keys) {
        String value = redisDataTemplate.opsForValue().get(key);
        resultList.add(value);
      }
    } catch (Exception e) {
      log.error("Error while fetching data from Redis: {}", e.getMessage(), e);
    }
    return resultList;
  }
}
