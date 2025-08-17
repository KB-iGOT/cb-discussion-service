package com.igot.cb.service.impl;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.igot.cb.authentication.util.AccessTokenValidator;
import com.igot.cb.discussion.entity.DiscussionAnswerPostReplyEntity;
import com.igot.cb.discussion.entity.DiscussionEntity;
import com.igot.cb.discussion.repository.DiscussionAnswerPostReplyRepository;
import com.igot.cb.discussion.repository.DiscussionRepository;
import com.igot.cb.discussion.service.impl.AnswerPostReplyServiceImpl;
import com.igot.cb.notificationUtill.HelperMethodService;
import com.igot.cb.notificationUtill.NotificationTriggerService;
import com.igot.cb.pores.cache.CacheService;
import com.igot.cb.pores.elasticsearch.dto.SearchResult;
import com.igot.cb.pores.elasticsearch.service.EsUtilService;
import com.igot.cb.pores.util.ApiResponse;
import com.igot.cb.pores.util.CbServerProperties;
import com.igot.cb.pores.util.Constants;
import com.igot.cb.pores.util.PayloadValidation;
import com.igot.cb.transactional.cassandrautils.CassandraOperation;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatchers;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.http.HttpStatus;
import org.springframework.test.util.ReflectionTestUtils;

import java.lang.reflect.Field;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.*;

import static com.igot.cb.pores.util.Constants.DISCUSSION_ID;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class AnswerPostReplyServiceImplTest {

    @InjectMocks
    private AnswerPostReplyServiceImpl service;

    @Mock private PayloadValidation payloadValidation;
    @Mock private DiscussionRepository discussionRepository;
    @Mock private CacheService cacheService;
    @Mock private EsUtilService esUtilService;
    @Mock private CbServerProperties cbServerProperties;
    @Mock private AccessTokenValidator accessTokenValidator;
    @Mock private DiscussionAnswerPostReplyRepository discussionAnswerPostReplyRepository;
    @Mock private CassandraOperation cassandraOperation;
    @Mock private ObjectMapper objectMapper;
    @Mock private RedisTemplate<String, SearchResult> redisTemplate;
    @Mock private ValueOperations<String, SearchResult> valueOperations;
    @Mock private HelperMethodService helperMethodService;
    @Mock private NotificationTriggerService notificationTriggerService;
    private final ObjectMapper realMapper = new ObjectMapper();

    private final String token = "validToken";
    private final String parentAnswerPostId = "parentPostId";
    private final String communityId = "community123";
    private final String userId = "user123";
    private final String discussionId = "disc123";

    @BeforeEach
    void setUp() throws NoSuchFieldException, IllegalAccessException {
        lenient().when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        lenient().when(cbServerProperties.getDiscussionEntity()).thenReturn("discussionIndex");
        lenient().when(cbServerProperties.getElasticDiscussionJsonPath()).thenReturn("jsonPath");
        Field objectMapperField = AnswerPostReplyServiceImpl.class.getDeclaredField("objectMapper");
        objectMapperField.setAccessible(true);
        objectMapperField.set(service, objectMapper);
        ReflectionTestUtils.setField(service, "objectMapper", objectMapper);
    }

    private JsonNode buildValidPayload() {
        ObjectNode node = new ObjectMapper().createObjectNode();
        node.put(Constants.PARENT_ANSWER_POST_ID, parentAnswerPostId);
        node.put(Constants.COMMUNITY_ID, communityId);
        node.put(Constants.PARENT_DISCUSSION_ID, "discussionId");
        return node;
    }

    private Map<String, Object> getValidPayload(String type, String discussionId) {
        Map<String, Object> payload = new HashMap<>();
        payload.put(Constants.TYPE, type);
        payload.put(DISCUSSION_ID, discussionId);
        return payload;
    }

    private DiscussionEntity mockDiscussionEntity() {
        ObjectNode data = new ObjectMapper().createObjectNode();
        data.put(Constants.TYPE, Constants.ANSWER_POST);
        data.put(Constants.STATUS, Constants.ACTIVE);
        data.put(Constants.COMMUNITY_ID, communityId);
        data.put(Constants.CREATED_BY, userId);
        return new DiscussionEntity("discussionId123", data, true,
                new Timestamp(System.currentTimeMillis()),
                new Timestamp(System.currentTimeMillis()));
    }

    private List<Map<String, Object>> mockCommunityDetails() {
        Map<String, Object> userMap = new HashMap<>();
        userMap.put(Constants.STATUS, true);
        return List.of(userMap);
    }


    @Test
    void testCreateAnswerPostReply_invalidToken() {
        JsonNode payload = buildValidPayload();
        when(accessTokenValidator.verifyUserToken(token)).thenReturn(Constants.UNAUTHORIZED);

        ApiResponse response = service.createAnswerPostReply(payload, token);

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
        assertEquals(Constants.INVALID_AUTH_TOKEN, response.getParams().getErrMsg());
    }

    @Test
    void testCreateAnswerPostReply_invalidDiscussion() {
        JsonNode payload = buildValidPayload();
        when(accessTokenValidator.verifyUserToken(token)).thenReturn(userId);
        when(discussionRepository.findById(parentAnswerPostId)).thenReturn(Optional.empty());

        ApiResponse response = service.createAnswerPostReply(payload, token);

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    void testCreateAnswerPostReply_invalidCommunity() {
        JsonNode payload = buildValidPayload();
        DiscussionEntity discussionEntity = mockDiscussionEntity();
        ((ObjectNode) discussionEntity.getData()).put(Constants.COMMUNITY_ID, "invalidCommunity");

        when(accessTokenValidator.verifyUserToken(token)).thenReturn(userId);
        when(discussionRepository.findById(parentAnswerPostId)).thenReturn(Optional.of(discussionEntity));

        ApiResponse response = service.createAnswerPostReply(payload, token);

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
        assertEquals(Constants.INVALID_COMMUNITY_ID, response.getParams().getErrMsg());
    }

    @Test
    void testCreateAnswerPostReply_exceptionThrown() {
        JsonNode payload = buildValidPayload();
        DiscussionEntity discussionEntity = mockDiscussionEntity();

        when(accessTokenValidator.verifyUserToken(token)).thenReturn(userId);
        when(discussionRepository.findById(parentAnswerPostId)).thenReturn(Optional.of(discussionEntity));
        when(cassandraOperation.getRecordsByPropertiesWithoutFiltering(any(), any(), any(), any(), any()))
                .thenThrow(new RuntimeException("DB Error"));

        ApiResponse response = service.createAnswerPostReply(payload, token);

        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getResponseCode());
    }

    @Test
    void testUpdateAnswerPostReply_shouldReturn400() throws Exception {
        String replyId = "reply123";
        JsonNode inputNode = new ObjectMapper().readTree("{\"id\":\"reply123\",\"isInitialUpload\":false, \"answerPostReplyId\":\"reply123\"}");

        ObjectNode existingData = new ObjectMapper().createObjectNode();
        existingData.put("type", "AnswerPostReply");
        existingData.put("status", "active");
        existingData.put("parentAnswerPostId", "parent1");
        existingData.put("communityId", "comm1");

        DiscussionAnswerPostReplyEntity entity = new DiscussionAnswerPostReplyEntity();
        entity.setIsActive(true);
        entity.setData(existingData);
        entity.setDiscussionId("disc123");

        when(accessTokenValidator.verifyUserToken(token)).thenReturn(userId);
        when(discussionAnswerPostReplyRepository.findById(replyId)).thenReturn(Optional.of(entity));

        ApiResponse response = service.updateAnswerPostReply(inputNode, token);

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    void testUpdateAnswerPostReply_invalidToken() throws Exception {
        JsonNode inputNode = new ObjectMapper().readTree("{\"id\":\"reply123\"}");
        when(accessTokenValidator.verifyUserToken("bad-token")).thenReturn(Constants.UNAUTHORIZED);

        ApiResponse response = service.updateAnswerPostReply(inputNode, "bad-token");

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
        assertEquals(Constants.INVALID_AUTH_TOKEN, response.getParams().getErrMsg());
    }

    @Test
    void testManagePost_entityNotFound() {
        when(accessTokenValidator.verifyUserToken("token")).thenReturn("userId");
        Map<String, Object> payload = getValidPayload(Constants.ANSWER_POST_REPLY, "id");
        when(discussionAnswerPostReplyRepository.findById("id")).thenReturn(Optional.empty());

        ApiResponse response = service.managePost(payload, "token", Constants.SUSPEND);
        assertEquals(HttpStatus.NOT_FOUND, response.getResponseCode());
        assertEquals(Constants.SUCCESS, response.getParams().getStatus());
    }

    @Test
    void testManagePost_invalidPayload() {
        when(accessTokenValidator.verifyUserToken("token")).thenReturn("userId");

        Map<String, Object> invalidPayload = new HashMap<>();
        ApiResponse response = service.managePost(invalidPayload, "token", Constants.SUSPEND);
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
        assertEquals(Constants.FAILED, response.getMessage());
    }

    @Test
    void testManagePost_entityInactive() {
        when(accessTokenValidator.verifyUserToken("token")).thenReturn("userId");

        Map<String, Object> payload = getValidPayload(Constants.QUESTION, "id");
        DiscussionEntity entity = mockDiscussionEntity();
        ArrayNode arrayNode = new ObjectMapper().createArrayNode().add("existingId");
        ((ObjectNode) entity.getData()).set(Constants.STATUS, arrayNode);
        entity.setIsActive(false);
        when(discussionRepository.findById("id")).thenReturn(Optional.of(entity));

        ApiResponse response = service.managePost(payload, "token", Constants.SUSPEND);
        assertEquals(HttpStatus.CONFLICT, response.getResponseCode());
        assertEquals(Constants.FAILED, response.getMessage());
    }

    @Test
    void testManagePost_alreadySuspended_statusAsTextNode() {
        objectMapper = new ObjectMapper(); // Ensure objectMapper is initialized
        ReflectionTestUtils.setField(service, "objectMapper", objectMapper); // inject if needed

        when(accessTokenValidator.verifyUserToken("token")).thenReturn("userId");

        Map<String, Object> payload = getValidPayload(Constants.ANSWER_POST_REPLY, "id");

        ObjectNode data = objectMapper.createObjectNode()
                .put(Constants.STATUS, Constants.SUSPENDED)
                .put(Constants.COMMUNITY_ID, "comm");

        DiscussionAnswerPostReplyEntity entity = new DiscussionAnswerPostReplyEntity();
        entity.setData(data);
        entity.setIsActive(true);
        entity.setDiscussionId("disc123");

        when(discussionAnswerPostReplyRepository.findById("id")).thenReturn(Optional.of(entity));

        ApiResponse response = service.managePost(payload, "token", Constants.SUSPEND);
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
        assertEquals(Constants.FAILED, response.getMessage());
    }


    @Test
    void testManagePost_success_suspend() {
        objectMapper = new ObjectMapper(); // Ensure objectMapper is initialized
        ReflectionTestUtils.setField(service, "objectMapper", objectMapper); // inject if needed
        when(accessTokenValidator.verifyUserToken("token")).thenReturn("adminUser");

        Map<String, Object> payload = getValidPayload(Constants.ANSWER_POST_REPLY, "id");
        ObjectNode data = objectMapper.createObjectNode()
                .put(Constants.STATUS, Constants.ACTIVE)
                .put(Constants.COMMUNITY_ID, "comm");
        DiscussionAnswerPostReplyEntity entity = new DiscussionAnswerPostReplyEntity();
        entity.setData(data);
        entity.setIsActive(true);

        when(discussionAnswerPostReplyRepository.findById("id")).thenReturn(Optional.of(entity));

        ApiResponse response = service.managePost(payload, "token", Constants.SUSPEND);
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
        assertEquals(Constants.FAILED, response.getMessage());
    }

    @Test
    void testManagePost_success_activate() {
        objectMapper = new ObjectMapper(); // Ensure objectMapper is initialized
        ReflectionTestUtils.setField(service, "objectMapper", objectMapper); // inject if needed
        when(accessTokenValidator.verifyUserToken("token")).thenReturn("adminUser");

        Map<String, Object> payload = getValidPayload(Constants.ANSWER_POST_REPLY, "id");
        ObjectNode data = objectMapper.createObjectNode()
                .put(Constants.STATUS, Constants.SUSPENDED)
                .put(Constants.COMMUNITY_ID, "comm");
        DiscussionAnswerPostReplyEntity entity = new DiscussionAnswerPostReplyEntity();
        entity.setData(data);
        entity.setIsActive(true);

        when(discussionAnswerPostReplyRepository.findById("id")).thenReturn(Optional.of(entity));
        when(cbServerProperties.getDiscussionEntity()).thenReturn("discussion");
        when(cbServerProperties.getElasticDiscussionJsonPath()).thenReturn("path");

        ApiResponse response = service.managePost(payload, "token", Constants.ACTIVE);
        assertEquals(HttpStatus.OK, response.getResponseCode());
        assertEquals(Constants.ADMIN_MANAGE_POST_API, response.getId());
    }

    @Test
    void testManagePost_withException() {
        when(accessTokenValidator.verifyUserToken("token")).thenReturn("adminUser");
        Map<String, Object> payload = getValidPayload(Constants.ANSWER_POST_REPLY, "id");
        when(discussionAnswerPostReplyRepository.findById("id"))
                .thenThrow(new RuntimeException("DB Error"));
        ApiResponse response = service.managePost(payload, "token", Constants.SUSPEND);
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getResponseCode());

    }

    @Test
    void testGetReportStatistics_withCachedData() {
        Map<String, Object> input = Map.of(DISCUSSION_ID, discussionId);
        ApiResponse response = service.getReportStatistics(input);

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
        assertEquals(Constants.FAILED, response.getMessage());
    }

    @Test
    void testGetReportStatistics_withValidReasonsAndCounts(){
        Map<String, Object> input = Map.of(DISCUSSION_ID, discussionId);

        ApiResponse response = service.getReportStatistics(input);

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
        assertEquals(Constants.FAILED, response.getMessage());
    }

    @Test
    void testGetReportStatistics_withNoReportReasons() {
        Map<String, Object> input = Map.of(DISCUSSION_ID, discussionId);

        ApiResponse response = service.getReportStatistics(input);

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
        assertEquals(Constants.FAILED, response.getMessage());
    }

    @Test
    void testGetReportStatistics_withEmptyConfigData() {
        Map<String, Object> input = Map.of(DISCUSSION_ID, discussionId);
        ApiResponse response = service.getReportStatistics(input);
        assertEquals(Constants.FAILED, response.getMessage());
    }

    @Test
    void testGetReportStatistics_withException(){
        Map<String, Object> input = Map.of(DISCUSSION_ID, discussionId);

        ApiResponse response = service.getReportStatistics(input);

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
        assertEquals(Constants.FAILED, response.getMessage());
    }

    @Test
    void testMigrateRecentReportedTime_successfulMigration() {
        Map<String, Object> record1 = Map.of(Constants.DISCUSSION_ID_KEY, discussionId,
                Constants.CREATED_ON_KEY, Instant.now());
        when(cassandraOperation.getRecordsByPropertiesByKey(any(), any(), any(), any(), any()))
                .thenReturn(List.of(record1));

        DiscussionEntity entity = new DiscussionEntity();
        entity.setData(new ObjectMapper().createObjectNode());

        when(discussionRepository.findById(discussionId)).thenReturn(Optional.of(entity));
        when(cbServerProperties.getDiscussionEntity()).thenReturn("discussionEntity");
        when(cbServerProperties.getElasticDiscussionJsonPath()).thenReturn("jsonPath");

        ApiResponse response = service.migrateRecentReportedTime();

        assertEquals(Constants.SUCCESS, response.getParams().getStatus());
        verify(discussionRepository).save(any());
        verify(esUtilService).updateDocument(any(), any(), any(), any());
    }

    @Test
    void testMigrateRecentReportedTime_discussionNotFound() {
        Map<String, Object> record1 = Map.of(Constants.DISCUSSION_ID_KEY, discussionId,
                Constants.CREATED_ON_KEY, Instant.now());
        when(cassandraOperation.getRecordsByPropertiesByKey(any(), any(), any(), any(), any()))
                .thenReturn(List.of(record1));

        when(discussionRepository.findById(discussionId)).thenReturn(Optional.empty());

        ApiResponse response = service.migrateRecentReportedTime();

        assertEquals(Constants.SUCCESS, response.getParams().getStatus());
    }

    @Test
    void testMigrateRecentReportedTime_withException() {
        when(cassandraOperation.getRecordsByPropertiesByKey(any(), any(), any(), any(), any()))
                .thenThrow(new RuntimeException("fail"));

        ApiResponse response = service.migrateRecentReportedTime();
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getResponseCode());
        assertEquals("migrate data failed", response.getParams().getErrMsg());
    }

    @Test
    void testReadFromCache() throws Exception {
        String cachedData = "{\"field\":\"value\"}";

        when(cacheService.getCache(Constants.DISCUSSION_CACHE_PREFIX + discussionId)).thenReturn(cachedData);
        when(objectMapper.readValue(eq(cachedData), ArgumentMatchers.<TypeReference<Object>>any())).thenReturn(Map.of("field", "value"));

        ApiResponse response = service.readAnswerPostReply(discussionId);
        assertEquals(HttpStatus.OK, response.getResponseCode());
    }

    @Test
    void testReadFromDatabase() {
        Map<String, Object> data = new HashMap<>();
        data.put("field", "value");

        var entity = new DiscussionAnswerPostReplyEntity();
        entity.setIsActive(true);
        entity.setCreatedOn(new Timestamp(new Date().getTime()));
        entity.setUpdatedOn(new Timestamp(new Date().getTime()));
        entity.setData(mock(JsonNode.class));
        entity.setDiscussionId(discussionId);

        when(cacheService.getCache(Constants.DISCUSSION_CACHE_PREFIX + discussionId)).thenReturn(null);
        when(discussionAnswerPostReplyRepository.findById(discussionId)).thenReturn(Optional.of(entity));
        when(objectMapper.convertValue(any(), ArgumentMatchers.<TypeReference<Object>>any())).thenReturn(data);

        ApiResponse response = service.readAnswerPostReply(discussionId);
        assertEquals(HttpStatus.OK, response.getResponseCode());
    }

    @Test
    void testReadNotFound() {
        when(cacheService.getCache(any())).thenReturn(null);
        when(discussionAnswerPostReplyRepository.findById("id")).thenReturn(Optional.empty());
        var response = service.readAnswerPostReply("id");
        assertEquals(HttpStatus.NOT_FOUND, response.getResponseCode());
    }

    @Test
    void testReadException() {
        when(cacheService.getCache(any())).thenThrow(new RuntimeException("fail"));
        var response = service.readAnswerPostReply("id");
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getResponseCode());
    }



    @Test
    void shouldReturn500whileDeleteAnswerPostReplye() {
        String type = "type";
        String parentAnswerPostId = "parent";

        var entity = new DiscussionAnswerPostReplyEntity();
        ObjectNode jsonNode = mock(ObjectNode.class);

        when(jsonNode.get(Constants.TYPE)).thenReturn(mock(JsonNode.class));
        when(jsonNode.get(Constants.PARENT_ANSWER_POST_ID)).thenReturn(mock(JsonNode.class));
        when(jsonNode.get(Constants.TYPE).asText()).thenReturn(type);
        when(jsonNode.get(Constants.PARENT_ANSWER_POST_ID).asText()).thenReturn(parentAnswerPostId);

        entity.setIsActive(true);
        entity.setData(jsonNode);
        when(accessTokenValidator.verifyUserToken(token)).thenReturn("user");
        when(discussionAnswerPostReplyRepository.findById(discussionId)).thenReturn(Optional.of(entity));
        when(discussionRepository.findById(parentAnswerPostId)).thenReturn(Optional.of(new DiscussionEntity()));
        when(objectMapper.convertValue(any(), ArgumentMatchers.<Class<Map>>any())).thenReturn(new HashMap<>());

        var response = service.deleteAnswerPostReply(discussionId, type, token);
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getResponseCode());
    }

    @Test
    void testDeleteInvalidToken() {
        var response = service.deleteAnswerPostReply("id", "type", null);
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
        assertEquals("invalid auth token Please supply a valid auth token", response.getParams().getErrMsg());
    }

    @Test
    void testDeleteInvalidId() {
        when(accessTokenValidator.verifyUserToken("token")).thenReturn("user");
        var response = service.deleteAnswerPostReply("", "type", "token");
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
        assertEquals("Id not found", response.getParams().getErrMsg());
    }

    @Test
    void testDeleteNotFound() {
        when(accessTokenValidator.verifyUserToken("token")).thenReturn("user");
        when(discussionAnswerPostReplyRepository.findById("id")).thenReturn(Optional.empty());
        var response = service.deleteAnswerPostReply("id", "type", "token");
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
        assertEquals("Invalid Id", response.getParams().getErrMsg());
    }

    @Test
    void testDeleteAlreadyInactive() {
        var entity = new DiscussionAnswerPostReplyEntity();
        entity.setIsActive(false);

        when(accessTokenValidator.verifyUserToken("token")).thenReturn("user");
        when(discussionAnswerPostReplyRepository.findById("id")).thenReturn(Optional.of(entity));

        var response = service.deleteAnswerPostReply("id", "type", "token");
        assertEquals(HttpStatus.ALREADY_REPORTED, response.getResponseCode());
    }

    @Test
    void testDeleteInvalidType() {
        var entity = new DiscussionAnswerPostReplyEntity();
        ObjectNode node = mock(ObjectNode.class);
        when(node.get(Constants.TYPE)).thenReturn(mock(JsonNode.class));
        when(node.get(Constants.TYPE).asText()).thenReturn("wrongType");
        entity.setIsActive(true);
        entity.setData(node);

        when(accessTokenValidator.verifyUserToken("token")).thenReturn("user");
        when(discussionAnswerPostReplyRepository.findById("id")).thenReturn(Optional.of(entity));

        var response = service.deleteAnswerPostReply("id", "type", "token");
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
        assertEquals("Invalid type : type", response.getParams().getErrMsg());
    }

    @Test
    void testDeleteException() {
        var entity = new DiscussionAnswerPostReplyEntity();
        entity.setIsActive(true);
        ObjectNode node = mock(ObjectNode.class);
        when(node.get(Constants.TYPE)).thenReturn(mock(JsonNode.class));
        when(node.get(Constants.TYPE).asText()).thenReturn("type");
        entity.setData(node);

        when(accessTokenValidator.verifyUserToken("token")).thenReturn("user");
        when(discussionAnswerPostReplyRepository.findById("id")).thenReturn(Optional.of(entity));

        var response = service.deleteAnswerPostReply("id", "type", "token");
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getResponseCode());
    }

    @Test
    void testGetReportStatistics_Success() throws Exception {
        Map<String, Object> input = new HashMap<>();
        input.put(DISCUSSION_ID, DISCUSSION_ID);
        input.put(Constants.TYPE, Constants.QUESTION);

        // No cache for report stats
        when(cacheService.getCache(Constants.REPORT_STATISTICS_CACHE_PREFIX + DISCUSSION_ID)).thenReturn(null);

        // Cache miss for valid reasons, so fallback to Cassandra
        when(cacheService.getCache(Constants.VALID_REASONS_CACHE_KEY)).thenReturn(null);

        List<Map<String, Object>> configData = new ArrayList<>();
        Map<String, Object> configEntry = new HashMap<>();
        Set<String> validReasons = Set.of("Spam", "Offensive");
        String validReasonsJson = new ObjectMapper().writeValueAsString(validReasons);
        configEntry.put(Constants.VALUE, validReasonsJson);
        configData.add(configEntry);

        when(cassandraOperation.getRecordsByPropertiesWithoutFiltering(
                eq(Constants.KEYSPACE_SUNBIRD),
                eq(Constants.SYSTEM_SETTINGS),
                anyMap(),
                eq(Arrays.asList(Constants.VALUE)),
                isNull()))
                .thenReturn(configData);

        List<Map<String, Object>> reportReasons = List.of(
                Map.of(Constants.REASON, "Spam"),
                Map.of(Constants.REASON, "Spam, Offensive"),
                Map.of(Constants.REASON, "Offensive")
        );

        when(cassandraOperation.getRecordsByPropertiesWithoutFiltering(
                eq(Constants.KEYSPACE_SUNBIRD),
                eq(Constants.DISCUSSION_POST_REPORT_LOOKUP_BY_POST),
                anyMap(),
                eq(Arrays.asList(Constants.REASON)),
                isNull()))
                .thenReturn(reportReasons);

        ApiResponse response = service.getReportStatistics(input);

        assertEquals(HttpStatus.OK, response.getResponseCode());
        assertEquals(Constants.SUCCESS, response.getParams().getStatus());
        assertNotNull(response.getResult());
        assertTrue(response.getResult().containsKey(Constants.TOTAL_COUNT));
        assertTrue(response.getResult().containsKey(Constants.REPORT_REASONS));
    }
    @Test
    void readAnswerPostReply_whenDiscussionIdIsBlank_shouldReturnBadRequestResponse() {
        // Arrange
        String blankDiscussionId = "  "; // blank input

        // Act
        ApiResponse response = service.readAnswerPostReply(blankDiscussionId);

        // Assert
        assertNotNull(response);
        assertEquals(Constants.ANSWER_POST_REPLY_READ_API, response.getId());
        assertEquals(Constants.FAILED, response.getParams().getStatus());
    }

    @Test
    void testDeleteAnswerPostReply_Success() {
        String token = "valid.token";
        String userId = "user123";
        String discussionId = "reply123";
        String type = "reply";

        // Mock token verification
        when(accessTokenValidator.verifyUserToken(token)).thenReturn(userId);

        // Prepare reply entity with JsonNode
        ObjectNode replyData = realMapper.createObjectNode();
        replyData.put(Constants.TYPE, type);
        replyData.put(Constants.PARENT_ANSWER_POST_ID, "parentAnsId");
        replyData.put(Constants.PARENT_DISCUSSION_ID, "parentDiscId");
        replyData.put(Constants.COMMUNITY_ID, "community1");
        replyData.put(Constants.IS_ACTIVE, true);

        DiscussionAnswerPostReplyEntity replyEntity = new DiscussionAnswerPostReplyEntity();
        replyEntity.setIsActive(true);
        replyEntity.setData(replyData);
        replyEntity.setDiscussionId(discussionId);

        when(discussionAnswerPostReplyRepository.findById(discussionId)).thenReturn(Optional.of(replyEntity));

        // Parent DiscussionEntity
        ObjectNode discussionData = realMapper.createObjectNode();
        ArrayNode answerReplies = realMapper.createArrayNode().add(discussionId);
        discussionData.set(Constants.ANSWER_POST_REPLIES, answerReplies);
        discussionData.put(Constants.ANSWER_POST_REPLIES_COUNT, 1);

        DiscussionEntity discussionEntity = new DiscussionEntity();
        discussionEntity.setDiscussionId("parentAnsId");
        discussionEntity.setData(discussionData);

        when(discussionRepository.findById("parentAnsId")).thenReturn(Optional.of(discussionEntity));
        when(discussionRepository.save(any())).thenReturn(discussionEntity);

        // Required property mocks
        when(cbServerProperties.getDiscussionEntity()).thenReturn("discussionEntity");
        when(cbServerProperties.getElasticDiscussionJsonPath()).thenReturn("path");

        // Mock valueToTree and convertValue
        when(objectMapper.valueToTree(any(Set.class))).thenReturn(realMapper.createArrayNode());
        when(objectMapper.createObjectNode()).thenReturn(realMapper.createObjectNode());
        when(objectMapper.convertValue(any(ObjectNode.class), eq(Map.class))).thenReturn(new HashMap<>());

        // Call actual method
        ApiResponse response = service.deleteAnswerPostReply(discussionId, type, token);

        assertEquals(HttpStatus.OK, response.getResponseCode());
        assertEquals(Constants.SUCCESS, response.getParams().getStatus());
        assertEquals(Constants.DELETED_SUCCESSFULLY, response.getMessage());

        verify(discussionAnswerPostReplyRepository).save(any());
        verify(esUtilService, atLeastOnce()).updateDocument(any(), any(), any(), any());
        verify(cacheService, atLeastOnce()).putCache(any(), any());
        verify(redisTemplate.opsForValue(), times(2)).getAndDelete(any());
    }

    @Test
    void testUpdateAnswerPostReply_FailureDuringUpdate() {
        String token = "valid.token";
        String userId = "user123";
        String answerPostReplyId = "reply123";

        ObjectNode inputData = new ObjectMapper().createObjectNode();
        inputData.put(Constants.ANSWER_POST_REPLY_ID, answerPostReplyId);
        inputData.put(Constants.IS_INITIAL_UPLOAD, false);

        // Mock valid token
        when(accessTokenValidator.verifyUserToken(token)).thenReturn(userId);

        // Mock entity from DB
        ObjectNode dbData = new ObjectMapper().createObjectNode();
        dbData.put(Constants.TYPE, Constants.ANSWER_POST_REPLY);
        dbData.put(Constants.STATUS, Constants.ACTIVE);
        dbData.put(Constants.PARENT_ANSWER_POST_ID, "parent123");
        dbData.put(Constants.COMMUNITY_ID, "communityX");

        DiscussionAnswerPostReplyEntity entity = new DiscussionAnswerPostReplyEntity();
        entity.setIsActive(true);
        entity.setData(dbData);
        entity.setDiscussionId("reply123");

        when(discussionAnswerPostReplyRepository.findById(answerPostReplyId)).thenReturn(Optional.of(entity));

        // Force exception in convertValue
        ApiResponse response = service.updateAnswerPostReply(inputData, token);

        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getResponseCode());
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(Constants.FAILED_TO_UPDATE_ANSWER_POST, response.getParams().getErrMsg());
    }
}

