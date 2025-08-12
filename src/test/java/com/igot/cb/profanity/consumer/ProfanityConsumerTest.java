package com.igot.cb.profanity.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.igot.cb.discussion.entity.DiscussionAnswerPostReplyEntity;
import com.igot.cb.discussion.entity.DiscussionEntity;
import com.igot.cb.discussion.repository.DiscussionAnswerPostReplyRepository;
import com.igot.cb.discussion.repository.DiscussionRepository;
import com.igot.cb.notificationUtill.HelperMethodService;
import com.igot.cb.notificationUtill.NotificationTriggerService;
import com.igot.cb.pores.elasticsearch.service.EsUtilService;
import com.igot.cb.pores.util.CbServerProperties;
import com.igot.cb.pores.util.Constants;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.*;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collections;
import java.util.HashMap;
import java.util.Optional;

import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class ProfanityConsumerTest {

    @InjectMocks
    private ProfanityConsumer profanityConsumer;

    @Mock
    private ObjectMapper mapper;

    @Mock
    private DiscussionRepository discussionRepository;

    @Mock
    private DiscussionAnswerPostReplyRepository discussionAnswerPostReplyRepository;

    @Captor
    private ArgumentCaptor<String> postIdCaptor;

    @Captor
    private ArgumentCaptor<String> responseCaptor;

    @Captor
    private ArgumentCaptor<Boolean> isProfaneCaptor;

    @Mock
    private EsUtilService esUtilService;

    @Mock
    private CbServerProperties cbServerProperties;

    @Mock
    private HelperMethodService helperMethodService;

    @Mock
    private NotificationTriggerService notificationTriggerService;

    private static final String BASE_JSON_TEMPLATE = """
            {
                "request_data": {
                    "metadata": {
                        "post_id": "%s",
                        "type": "%s"
                    }
                },
                "response_data": {
                    "response": {
                        "isProfane": %s
                    }
                }
            }
            """;

    @Test
    void testCheckTextContentIsProfane_QuestionType() throws Exception {
        String kafkaValue = BASE_JSON_TEMPLATE.formatted("post123", Constants.QUESTION, true);
        ConsumerRecord<String, String> consumerRecord = new ConsumerRecord<>("topic", 0, 0L, null, kafkaValue);
        JsonNode mockNode = mockJsonTree(kafkaValue, "post123", Constants.QUESTION, true);
        when(mapper.readTree(kafkaValue)).thenReturn(mockNode);
        when(mockNode.toString()).thenReturn(kafkaValue);
        profanityConsumer.checkTextContentIsProfane(consumerRecord);
        Thread.sleep(200);
        verify(discussionRepository).updateProfanityFieldsByDiscussionId("post123", kafkaValue, true);
        verifyNoInteractions(discussionAnswerPostReplyRepository);
    }

    @Test
    void testCheckTextContentIsProfane_AnswerPostType() throws Exception {
        String kafkaValue = BASE_JSON_TEMPLATE.formatted("post456", Constants.ANSWER_POST, false);
        ConsumerRecord<String, String> consumerRecord = new ConsumerRecord<>("topic", 0, 0L, null, kafkaValue);
        JsonNode mockNode = mockJsonTree(kafkaValue, "post456", Constants.ANSWER_POST, false);
        when(mapper.readTree(kafkaValue)).thenReturn(mockNode);
        when(mockNode.toString()).thenReturn(kafkaValue);
        profanityConsumer.checkTextContentIsProfane(consumerRecord);
        Thread.sleep(200);
        verify(discussionRepository).updateProfanityFieldsByDiscussionId("post456", kafkaValue, false);
        verifyNoInteractions(discussionAnswerPostReplyRepository);
    }

    @Test
    void testCheckTextContentIsProfane_AnswerPostReplyType() throws Exception {
        String kafkaValue = BASE_JSON_TEMPLATE.formatted("reply123", Constants.ANSWER_POST_REPLY, true);
        ConsumerRecord<String, String> consumerRecord = new ConsumerRecord<>("topic", 0, 0L, null, kafkaValue);
        JsonNode mockNode = mockJsonTree(kafkaValue, "reply123", Constants.ANSWER_POST_REPLY, true);
        when(mapper.readTree(kafkaValue)).thenReturn(mockNode);
        when(mockNode.toString()).thenReturn(kafkaValue);
        profanityConsumer.checkTextContentIsProfane(consumerRecord);
        Thread.sleep(200);
        verify(discussionAnswerPostReplyRepository).updateProfanityFieldsByDiscussionId("reply123", kafkaValue, true);
        verifyNoInteractions(discussionRepository);
    }

    @Test
    void testCheckTextContentIsProfane_UnknownType_NoUpdate() throws Exception {
        String kafkaValue = BASE_JSON_TEMPLATE.formatted("unknown789", "UNKNOWN_TYPE", true);
        ConsumerRecord<String, String> consumerRecord = new ConsumerRecord<>("topic", 0, 0L, null, kafkaValue);
        JsonNode mockNode = mockJsonTree(kafkaValue, "unknown789", "UNKNOWN_TYPE", true);
        when(mapper.readTree(kafkaValue)).thenReturn(mockNode);
        when(mockNode.toString()).thenReturn(kafkaValue);
        profanityConsumer.checkTextContentIsProfane(consumerRecord);
        Thread.sleep(200);
        verifyNoInteractions(discussionRepository);
        verifyNoInteractions(discussionAnswerPostReplyRepository);
    }

    @Test
    void testCheckTextContentIsProfane_InvalidJson() throws Exception {
        ConsumerRecord<String, String> consumerRecord = new ConsumerRecord<>("topic", 0, 0L, null, "invalid_json");
        when(mapper.readTree("invalid_json")).thenThrow(new JsonProcessingException("error") {
        });
        profanityConsumer.checkTextContentIsProfane(consumerRecord);
        verifyNoInteractions(discussionRepository);
        verifyNoInteractions(discussionAnswerPostReplyRepository);
    }

    @Test
    void testCheckTextContentIsProfane_EmptyValue() {
        ConsumerRecord<String, String> consumerRecord = new ConsumerRecord<>("topic", 0, 0L, null, "");
        profanityConsumer.checkTextContentIsProfane(consumerRecord);
        verifyNoInteractions(discussionRepository);
        verifyNoInteractions(discussionAnswerPostReplyRepository);
    }

    @Test
    void testCheckTextContentIsProfane_DiscussionRepositoryThrowsException() throws Exception {
        String kafkaValue = BASE_JSON_TEMPLATE.formatted("postException", Constants.QUESTION, true);
        ConsumerRecord<String, String> consumerRecord = new ConsumerRecord<>("topic", 0, 0L, null, kafkaValue);
        JsonNode mockNode = mockJsonTree(kafkaValue, "postException", Constants.QUESTION, true);
        when(mapper.readTree(kafkaValue)).thenReturn(mockNode);
        when(mockNode.toString()).thenReturn(kafkaValue);
        doThrow(new RuntimeException("DB failure")).when(discussionRepository)
                .updateProfanityFieldsByDiscussionId(anyString(), anyString(), anyBoolean());
        profanityConsumer.checkTextContentIsProfane(consumerRecord);
        Thread.sleep(200);
        verify(discussionRepository).updateProfanityFieldsByDiscussionId("postException", kafkaValue, true);
    }

    private JsonNode mockJsonTree(String json, String postId, String type, boolean isProfane) {
        JsonNode mockRoot = mock(JsonNode.class);
        JsonNode requestData = mock(JsonNode.class);
        JsonNode metadata = mock(JsonNode.class);
        JsonNode postIdNode = mock(JsonNode.class);
        JsonNode typeNode = mock(JsonNode.class);
        JsonNode responseData = mock(JsonNode.class);
        JsonNode responsePath = mock(JsonNode.class);
        JsonNode isProfaneNode = mock(JsonNode.class);
        when(mockRoot.path(Constants.REQUEST_DATA)).thenReturn(requestData);
        when(requestData.path(Constants.METADATA)).thenReturn(metadata);
        when(metadata.path(Constants.POST_ID)).thenReturn(postIdNode);
        when(metadata.path(Constants.TYPE)).thenReturn(typeNode);
        when(postIdNode.asText()).thenReturn(postId);
        when(typeNode.asText()).thenReturn(type);
        when(mockRoot.path(Constants.RESPONSE_DATA)).thenReturn(responseData);
        when(responseData.path(Constants.RESPONSE_DATA_PATH)).thenReturn(responsePath);
        when(responsePath.path(Constants.IS_PROFANE)).thenReturn(isProfaneNode);
        when(isProfaneNode.asBoolean(false)).thenReturn(isProfane);
        return mockRoot;
    }

    @Test
    void testSyncProfaneDetailsToESForDiscussion_InactiveDiscussion_SkipsES() throws Exception {
        String discussionId = "d2";
        DiscussionEntity entity = mock(DiscussionEntity.class);
        when(discussionRepository.findById(discussionId)).thenReturn(Optional.of(entity));
        when(entity.getIsActive()).thenReturn(false);
        var method = ProfanityConsumer.class.getDeclaredMethod("syncProfaneDetailsToESForDiscussion", String.class, boolean.class, String.class);
        method.setAccessible(true);
        method.invoke(profanityConsumer, discussionId, true, "question");
        verifyNoInteractions(esUtilService);
    }

    @Test
    void testSyncProfaneDetailsToESForDiscussion_DiscussionNotFound_LogsWarning() throws Exception {
        String discussionId = "d3";
        when(discussionRepository.findById(discussionId)).thenReturn(Optional.empty());
        var method = ProfanityConsumer.class.getDeclaredMethod("syncProfaneDetailsToESForDiscussion", String.class, boolean.class, String.class);
        method.setAccessible(true);
        method.invoke(profanityConsumer, discussionId, true, "question");
        verifyNoInteractions(esUtilService);
    }

    @Test
    void testSyncProfaneDetailsToESForAnswerPost_NotFound_SkipsES() throws Exception {
        String discussionId = "reply3";
        when(discussionAnswerPostReplyRepository.findById(discussionId)).thenReturn(Optional.empty());
        var method = ProfanityConsumer.class.getDeclaredMethod("syncProfaneDetailsToESForAnswerPost", String.class, boolean.class, String.class);
        method.setAccessible(true);
        method.invoke(profanityConsumer, discussionId, true, "answer_post_reply");
        verifyNoInteractions(esUtilService);
    }

    // Java
    @Test
    void testSyncProfaneDetailsToESForDiscussion_DiscussionNotFound() throws Exception {
        String discussionId = "notfound";
        when(discussionRepository.findById(discussionId)).thenReturn(Optional.empty());
        var method = ProfanityConsumer.class.getDeclaredMethod("syncProfaneDetailsToESForDiscussion", String.class, boolean.class, String.class);
        method.setAccessible(true);
        method.invoke(profanityConsumer, discussionId, true, "question");
        verifyNoInteractions(esUtilService);
        verifyNoInteractions(notificationTriggerService);
    }

    @Test
    void testSyncProfaneDetailsToESForDiscussion_InactiveDiscussion() throws Exception {
        String discussionId = "inactive";
        DiscussionEntity entity = mock(DiscussionEntity.class);
        when(discussionRepository.findById(discussionId)).thenReturn(Optional.of(entity));
        when(entity.getIsActive()).thenReturn(false);
        var method = ProfanityConsumer.class.getDeclaredMethod("syncProfaneDetailsToESForDiscussion", String.class, boolean.class, String.class);
        method.setAccessible(true);
        method.invoke(profanityConsumer, discussionId, true, "question");
        verifyNoInteractions(esUtilService);
        verifyNoInteractions(notificationTriggerService);
    }

    @Test
    void testSyncProfaneDetailsToESForDiscussion_Active_NotProfane() throws Exception {
        String discussionId = "activeNotProfane";
        ObjectNode data = JsonNodeFactory.instance.objectNode();
        data.put("createdBy", "user123");
        data.put(Constants.COMMUNITY_ID, "community1");
        data.put(Constants.DISCUSSION_ID, discussionId);
        DiscussionEntity entity = mock(DiscussionEntity.class);
        when(discussionRepository.findById(discussionId)).thenReturn(Optional.of(entity));
        when(entity.getIsActive()).thenReturn(true);
        when(entity.getData()).thenReturn(data);
        when(entity.getDiscussionId()).thenReturn(discussionId);
        when(cbServerProperties.getDiscussionEntity()).thenReturn("discussionEntity");
        when(cbServerProperties.getElasticDiscussionJsonPath()).thenReturn("jsonPath");
        when(mapper.convertValue(eq(data), any(TypeReference.class))).thenReturn(new HashMap<>());
        var method = ProfanityConsumer.class.getDeclaredMethod("syncProfaneDetailsToESForDiscussion", String.class, boolean.class, String.class);
        method.setAccessible(true);
        var objectMapperField = ProfanityConsumer.class.getDeclaredField("objectMapper");
        objectMapperField.setAccessible(true);
        objectMapperField.set(profanityConsumer, mapper);
        method.invoke(profanityConsumer, discussionId, false, "question");
        verify(esUtilService).updateDocument(eq("discussionEntity"), eq(discussionId), anyMap(), eq("jsonPath"));
        verifyNoInteractions(notificationTriggerService);
    }

    @Test
    void testSyncProfaneDetailsToESForDiscussion_Active_Profane_TriggersNotification() throws Exception {
        String discussionId = "activeProfane";
        ObjectNode data = JsonNodeFactory.instance.objectNode();
        data.put("createdBy", "user123");
        data.put(Constants.COMMUNITY_ID, "community1");
        data.put(Constants.DISCUSSION_ID, discussionId);
        DiscussionEntity entity = mock(DiscussionEntity.class);
        when(discussionRepository.findById(discussionId)).thenReturn(Optional.of(entity));
        when(entity.getIsActive()).thenReturn(true);
        when(entity.getData()).thenReturn(data);
        when(entity.getDiscussionId()).thenReturn(discussionId);
        when(cbServerProperties.getDiscussionEntity()).thenReturn("discussionEntity");
        when(cbServerProperties.getElasticDiscussionJsonPath()).thenReturn("jsonPath");
        when(mapper.convertValue(eq(data), any(TypeReference.class))).thenReturn(new HashMap<>());
        when(helperMethodService.fetchUserFirstName("user123")).thenReturn("TestUser");
        var method = ProfanityConsumer.class.getDeclaredMethod("syncProfaneDetailsToESForDiscussion", String.class, boolean.class, String.class);
        method.setAccessible(true);
        var objectMapperField = ProfanityConsumer.class.getDeclaredField("objectMapper");
        objectMapperField.setAccessible(true);
        objectMapperField.set(profanityConsumer, mapper);
        method.invoke(profanityConsumer, discussionId, true, "question");
        verify(esUtilService).updateDocument(eq("discussionEntity"), eq(discussionId), anyMap(), eq("jsonPath"));
        verify(notificationTriggerService).triggerNotification(
                eq(Constants.PROFANITY_CHECK),
                eq(Constants.ALERT),
                eq(Collections.singletonList("user123")),
                eq(Constants.TITLE),
                eq("TestUser"),
                argThat(map -> Boolean.TRUE.equals(map.get(Constants.IS_PROFANE)) &&
                        "community1".equals(map.get(Constants.COMMUNITY_ID)) &&
                        discussionId.equals(map.get(Constants.DISCUSSION_ID)))
        );
    }

    @Test
    void testSyncProfaneDetailsToESForAnswerPost_NotFound() throws Exception {
        String discussionId = "notfound";
        when(discussionAnswerPostReplyRepository.findById(discussionId)).thenReturn(Optional.empty());
        var method = ProfanityConsumer.class.getDeclaredMethod("syncProfaneDetailsToESForAnswerPost", String.class, boolean.class, String.class);
        method.setAccessible(true);
        method.invoke(profanityConsumer, discussionId, true, "answer_post_reply");
        verifyNoInteractions(esUtilService);
        verifyNoInteractions(notificationTriggerService);
    }

    @Test
    void testSyncProfaneDetailsToESForAnswerPost_Inactive() throws Exception {
        String discussionId = "inactive";
        DiscussionAnswerPostReplyEntity entity = mock(DiscussionAnswerPostReplyEntity.class);
        when(discussionAnswerPostReplyRepository.findById(discussionId)).thenReturn(Optional.of(entity));
        when(entity.getIsActive()).thenReturn(false);
        var method = ProfanityConsumer.class.getDeclaredMethod("syncProfaneDetailsToESForAnswerPost", String.class, boolean.class, String.class);
        method.setAccessible(true);
        method.invoke(profanityConsumer, discussionId, true, "answer_post_reply");
        verifyNoInteractions(esUtilService);
        verifyNoInteractions(notificationTriggerService);
    }

    @Test
    void testSyncProfaneDetailsToESForAnswerPost_Active_NotProfane() throws Exception {
        String discussionId = "activeNotProfane";
        ObjectNode data = JsonNodeFactory.instance.objectNode();
        data.put("createdBy", "user123");
        data.put(Constants.COMMUNITY_ID, "community1");
        data.put(Constants.DISCUSSION_ID, discussionId);
        DiscussionAnswerPostReplyEntity entity = mock(DiscussionAnswerPostReplyEntity.class);
        when(discussionAnswerPostReplyRepository.findById(discussionId)).thenReturn(Optional.of(entity));
        when(entity.getIsActive()).thenReturn(true);
        when(entity.getData()).thenReturn(data);
        when(entity.getDiscussionId()).thenReturn(discussionId);
        when(cbServerProperties.getDiscussionEntity()).thenReturn("discussionEntity");
        when(cbServerProperties.getElasticDiscussionJsonPath()).thenReturn("jsonPath");
        when(mapper.convertValue(eq(data), any(TypeReference.class))).thenReturn(new HashMap<>());
        var method = ProfanityConsumer.class.getDeclaredMethod("syncProfaneDetailsToESForAnswerPost", String.class, boolean.class, String.class);
        method.setAccessible(true);
        var objectMapperField = ProfanityConsumer.class.getDeclaredField("objectMapper");
        objectMapperField.setAccessible(true);
        objectMapperField.set(profanityConsumer, mapper);
        method.invoke(profanityConsumer, discussionId, false, "answer_post_reply");
        verify(esUtilService).updateDocument(eq("discussionEntity"), eq(discussionId), anyMap(), eq("jsonPath"));
        verifyNoInteractions(notificationTriggerService);
    }

    @Test
    void testSyncProfaneDetailsToESForAnswerPost_Active_Profane_TriggersNotification() throws Exception {
        String discussionId = "activeProfane";
        ObjectNode data = JsonNodeFactory.instance.objectNode();
        data.put("createdBy", "user123");
        data.put(Constants.COMMUNITY_ID, "community1");
        data.put(Constants.DISCUSSION_ID, discussionId);
        DiscussionAnswerPostReplyEntity entity = mock(DiscussionAnswerPostReplyEntity.class);
        when(discussionAnswerPostReplyRepository.findById(discussionId)).thenReturn(Optional.of(entity));
        when(entity.getIsActive()).thenReturn(true);
        when(entity.getData()).thenReturn(data);
        when(entity.getDiscussionId()).thenReturn(discussionId);
        when(cbServerProperties.getDiscussionEntity()).thenReturn("discussionEntity");
        when(cbServerProperties.getElasticDiscussionJsonPath()).thenReturn("jsonPath");
        when(mapper.convertValue(eq(data), any(TypeReference.class))).thenReturn(new HashMap<>());
        when(helperMethodService.fetchUserFirstName("user123")).thenReturn("TestUser");
        var method = ProfanityConsumer.class.getDeclaredMethod("syncProfaneDetailsToESForAnswerPost", String.class, boolean.class, String.class);
        method.setAccessible(true);
        var objectMapperField = ProfanityConsumer.class.getDeclaredField("objectMapper");
        objectMapperField.setAccessible(true);
        objectMapperField.set(profanityConsumer, mapper);
        method.invoke(profanityConsumer, discussionId, true, "answer_post_reply");
        verify(esUtilService).updateDocument(eq("discussionEntity"), eq(discussionId), anyMap(), eq("jsonPath"));
        verify(notificationTriggerService).triggerNotification(
                eq(Constants.PROFANITY_CHECK),
                eq(Constants.ALERT),
                eq(Collections.singletonList("user123")),
                eq(Constants.TITLE),
                eq("TestUser"),
                argThat(map -> Boolean.TRUE.equals(map.get(Constants.IS_PROFANE)) &&
                        "community1".equals(map.get(Constants.COMMUNITY_ID)) &&
                        discussionId.equals(map.get(Constants.DISCUSSION_ID)))
        );
    }
}