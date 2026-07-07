package com.igot.cb.discussion.service;

public interface RateLimitingService {
    /**
     * Checks if the user has exceeded the maximum limit of answer posts.
     *
     * @param userId the ID of the user
     * @return true if the rate limit is exceeded, false otherwise
     */
    boolean isRateLimitExceeded(String userId);

    /**
     * Checks if the user has exceeded the rate limit for a specific feature.
     *
     * @param userId the ID of the user
     * @param featureKey a unique prefix/identifier for the feature
     * @param limit the configured maximum number of requests allowed
     * @return true if the rate limit is exceeded, false otherwise
     */
    boolean isRateLimitExceeded(String userId, String featureKey, int limit);

    void incrementAnswerPostCount(String userId);

    /**
     * Increments the rate limit counter for a specific feature.
     *
     * @param userId the ID of the user
     * @param featureKey a unique prefix/identifier for the feature
     * @param ttlSeconds the configured TTL for the window in seconds
     */
    void incrementCount(String userId, String featureKey, long ttlSeconds);
}
