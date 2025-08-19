package com.igot.cb.profanity.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.igot.cb.discussion.entity.DiscussionAnswerPostReplyEntity;
import com.igot.cb.discussion.entity.DiscussionEntity;
import com.igot.cb.discussion.repository.DiscussionAnswerPostReplyRepository;
import com.igot.cb.discussion.repository.DiscussionRepository;
import com.igot.cb.discussion.service.DiscussionService;
import com.igot.cb.notificationUtill.HelperMethodService;
import com.igot.cb.notificationUtill.NotificationTriggerService;
import com.igot.cb.pores.elasticsearch.dto.SearchResult;
import com.igot.cb.pores.elasticsearch.service.EsUtilService;
import com.igot.cb.pores.util.CbServerProperties;
import com.igot.cb.pores.util.Constants;
import com.igot.cb.transactional.cassandrautils.CassandraOperation;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.util.*;
import java.util.concurrent.CompletableFuture;

import static com.igot.cb.pores.util.Constants.*;

@Component
@Slf4j
public class ProfanityConsumer {

    @Autowired
    private ObjectMapper mapper;

    @Autowired
    private DiscussionRepository discussionRepository;

    @Autowired
    private EsUtilService esUtilService;

    @Autowired
    private CbServerProperties cbServerProperties;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private DiscussionService discussionService;

    @Autowired
    private CassandraOperation cassandraOperation;

    @Autowired
    @Qualifier(Constants.SEARCH_RESULT_REDIS_TEMPLATE)
    private RedisTemplate<String, SearchResult> redisTemplate;

    @Autowired
    private DiscussionAnswerPostReplyRepository discussionAnswerPostReplyRepository;

    @Autowired
    private NotificationTriggerService notificationTriggerService;

    @Autowired
    private HelperMethodService helperMethodService;

    /**
     * Kafka listener for processing profanity check messages.
     * Extracts contentId and isProfane from the message and updates the discussion record.
     *
     * @param textData the Kafka message containing JSON data
     */
    @KafkaListener(topics = "${kafka.topic.process.check.content.profanity}", groupId = "${kafka.group.process.check.content.profanity}")
    public void checkTextContentIsProfane(ConsumerRecord<String, String> textData) {
        if (StringUtils.hasText(textData.value())) {
            try {
                JsonNode textDataNode = mapper.readTree(textData.value());
                String discussionId = textDataNode.path(Constants.REQUEST_DATA).path(Constants.METADATA).path(Constants.POST_ID).asText();
                String type = textDataNode.path(Constants.REQUEST_DATA).path(Constants.METADATA).path(Constants.TYPE).asText();
                boolean isProfane = textDataNode.path(Constants.RESPONSE_DATA).path(Constants.RESPONSE_DATA_PATH).path(Constants.IS_PROFANE).asBoolean(false);
                String profanityResponseJson = textDataNode.toString();
                CompletableFuture.runAsync(() -> {
                    try {
                        if (Constants.QUESTION.equalsIgnoreCase(type) || Constants.ANSWER_POST.equalsIgnoreCase(type)) {
                            discussionRepository.updateProfanityFieldsByDiscussionId(discussionId, profanityResponseJson, isProfane);
                            log.info("Successfully updated profanity fields for Discussion: {}", discussionId);
                            syncProfaneDetailsToESForDiscussion(discussionId, isProfane, type);
                        } else if (Constants.ANSWER_POST_REPLY.equalsIgnoreCase(type)){
                            discussionAnswerPostReplyRepository.updateProfanityFieldsByDiscussionId(discussionId, profanityResponseJson, isProfane);
                            log.info("Successfully updated profanity fields for Answer Post Reply: {}", discussionId);
                            syncProfaneDetailsToESForAnswerPost(discussionId, isProfane, type);
                        }
                    } catch (Exception ex) {
                        log.error("Failed to update profanity fields for Discussion: {}", discussionId, ex);
                    }
                });
            } catch (JsonProcessingException e) {
                log.error("Failed to parse JSON from Kafka message: {}", textData.value(), e);
            }
        }
    }

    /**
     * Sync the profane details to Elasticsearch and deletes relevant caches.
     *
     * @param discussionId the ID of the discussion
     * @param isProfane    indicates whether the discussion is profane
     * @param type - the type of the post (e.g., question, answerPost)
     */
    private void syncProfaneDetailsToESForDiscussion(String discussionId, boolean isProfane, String type) {
        Optional<DiscussionEntity> discussionEntity = discussionRepository.findById(discussionId);
        if (discussionEntity.isPresent()) {
            DiscussionEntity discussionDbData = discussionEntity.get();
            if (Boolean.FALSE.equals(discussionDbData.getIsActive())) {
                log.info("Discussion is inactive, skipping Elasticsearch update for PostId: {}", discussionId);
            } else {
                ObjectNode data = (ObjectNode) discussionDbData.getData();
                Map<String, Object> map = objectMapper.convertValue(data, new TypeReference<Map<String, Object>>() {
                });
                map.put(Constants.IS_PROFANE, isProfane);
                esUtilService.updateDocument(cbServerProperties.getDiscussionEntity(), discussionDbData.getDiscussionId(), map, cbServerProperties.getElasticDiscussionJsonPath());
                if (isProfane) {
                    String userId = discussionDbData.getData().get("createdBy").asText();
                    String firstName = helperMethodService.fetchUserFirstName(userId);
                    Map<String, Object> notificationData = Map.of(
                            Constants.COMMUNITY_ID, data.get(Constants.COMMUNITY_ID).asText(),
                            Constants.DISCUSSION_ID, data.get(Constants.DISCUSSION_ID).asText(),
                            IS_PROFANE, true
                    );
                    notificationTriggerService.triggerNotification(Constants.PROFANITY_CHECK, ALERT, Collections.singletonList(userId), TITLE, firstName, notificationData);
                    discussionService.deleteCacheByCommunity(Constants.DISCUSSION_CACHE_PREFIX + data.get(Constants.COMMUNITY_ID).asText());
                    discussionService.deleteCacheByCommunity(Constants.DISCUSSION_POSTS_BY_USER + data.get(Constants.COMMUNITY_ID).asText() + Constants.UNDER_SCORE + userId);
                    discussionService.updateCacheForFirstFivePages(data.get(Constants.COMMUNITY_ID).asText(), false);
                }
            }
        } else {
            log.warn("Discussion not found for Discussion Id: {}", discussionId);
        }
    }

    /**
     * Sync the profane details to Elasticsearch for answer post replies and deletes relevant caches.
     *
     * @param discussionId the ID of the discussion answer post reply
     * @param isProfane    indicates whether the answer post reply is profane
     * @param type - the type of the post (e.g., answerPostReply)
     */
    private void syncProfaneDetailsToESForAnswerPost(String discussionId, boolean isProfane, String type) {
        Optional<DiscussionAnswerPostReplyEntity> discussionAnswerPostReplyEntity = discussionAnswerPostReplyRepository.findById(discussionId);
        if (discussionAnswerPostReplyEntity.isPresent()) {
            DiscussionAnswerPostReplyEntity discussionAnswerPostReply = discussionAnswerPostReplyEntity.get();
            if (Boolean.FALSE.equals(discussionAnswerPostReply.getIsActive())) {
                log.info("Discussion Answer Post Reply is inactive, skipping Elasticsearch update for PostId: {}", discussionId);
            } else {
                ObjectNode data = (ObjectNode) discussionAnswerPostReply.getData();
                Map<String, Object> map = objectMapper.convertValue(data, new TypeReference<Map<String, Object>>() {
                });
                map.put(Constants.IS_PROFANE, isProfane);
                esUtilService.updateDocument(cbServerProperties.getDiscussionEntity(), discussionAnswerPostReply.getDiscussionId(), map, cbServerProperties.getElasticDiscussionJsonPath());
                if (isProfane) {
                    String userId = discussionAnswerPostReply.getData().get("createdBy").asText();
                    String firstName = helperMethodService.fetchUserFirstName(userId);
                    Map<String, Object> notificationData = Map.of(
                            Constants.COMMUNITY_ID, data.get(Constants.COMMUNITY_ID).asText(),
                            Constants.DISCUSSION_ID, data.get(Constants.DISCUSSION_ID).asText(),
                            IS_PROFANE, true
                    );
                    notificationTriggerService.triggerNotification(Constants.PROFANITY_CHECK, ALERT, Collections.singletonList(userId), TITLE, firstName, notificationData);
                }
            }
        } else {
            log.warn("Discussion Answer Post Reply not found for Discussion Id: {}", discussionId);
        }
    }

}
 