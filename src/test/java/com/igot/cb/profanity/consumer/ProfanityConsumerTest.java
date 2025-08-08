package com.igot.cb.profanity.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.discussion.repository.DiscussionRepository;
import com.igot.cb.pores.util.Constants;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.*;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class ProfanityConsumerTest {

    @InjectMocks
    private ProfanityConsumer profanityConsumer;

    @Mock
    private ObjectMapper mapper;

    @Mock
    private DiscussionRepository discussionRepository;

    @Captor
    private ArgumentCaptor<String> postIdCaptor;

    @Captor
    private ArgumentCaptor<String> responseCaptor;

    @Captor
    private ArgumentCaptor<Boolean> isProfaneCaptor;

    private static final String KAFKA_VALUE = "{\"request_data\":{\"metadata\":{\"post_id\":\"post123\"}},\"response_data\":{\"response\":{\"isProfane\":true}}}";

    @Test
    void testCheckTextContentIsProfane_Success() throws Exception {
        ConsumerRecord<String, String> consumerRecord = new ConsumerRecord<>("topic", 0, 0L, null, KAFKA_VALUE);

        JsonNode mockNode = mock(JsonNode.class);
        JsonNode requestData = mock(JsonNode.class);
        JsonNode metadata = mock(JsonNode.class);
        JsonNode responseData = mock(JsonNode.class);
        JsonNode responsePath = mock(JsonNode.class);

        when(mapper.readTree(KAFKA_VALUE)).thenReturn(mockNode);
        when(mockNode.path(Constants.REQUEST_DATA)).thenReturn(requestData);
        when(requestData.path(Constants.METADATA)).thenReturn(metadata);
        when(metadata.path(Constants.POST_ID)).thenReturn(mock(JsonNode.class));
        when(metadata.path(Constants.POST_ID).asText()).thenReturn("post123");

        when(mockNode.path(Constants.RESPONSE_DATA)).thenReturn(responseData);
        when(responseData.path(Constants.RESPONSE_DATA_PATH)).thenReturn(responsePath);
        when(responsePath.path(Constants.IS_PROFANE)).thenReturn(mock(JsonNode.class));
        when(responsePath.path(Constants.IS_PROFANE).asBoolean(false)).thenReturn(true);

        when(mockNode.toString()).thenReturn(KAFKA_VALUE);

        profanityConsumer.checkTextContentIsProfane(consumerRecord);

        // Wait for async execution
        Thread.sleep(200);

        verify(discussionRepository, atLeastOnce()).updateProfanityFieldsByDiscussionId(
                eq("post123"), eq(KAFKA_VALUE), eq(true)
        );
    }

    @Test
    void testCheckTextContentIsProfane_InvalidJson() throws Exception {
        ConsumerRecord<String, String> consumerRecord = new ConsumerRecord<>("topic", 0, 0L, null, "invalid_json");
        when(mapper.readTree("invalid_json")).thenThrow(new JsonProcessingException("error") {});

        profanityConsumer.checkTextContentIsProfane(consumerRecord);

        verifyNoInteractions(discussionRepository);
    }

    @Test
    void testCheckTextContentIsProfane_EmptyValue() {
        ConsumerRecord<String, String> consumerRecord = new ConsumerRecord<>("topic", 0, 0L, null, "");
        profanityConsumer.checkTextContentIsProfane(consumerRecord);
        verifyNoInteractions(discussionRepository);
    }

    @Test
    void testCheckTextContentIsProfane_RepositoryThrowsException_LogsError() throws Exception {
        ConsumerRecord<String, String> consumerRecord = new ConsumerRecord<>("topic", 0, 0L, null, KAFKA_VALUE);

        JsonNode mockNode = mock(JsonNode.class);
        JsonNode requestData = mock(JsonNode.class);
        JsonNode metadata = mock(JsonNode.class);
        JsonNode responseData = mock(JsonNode.class);
        JsonNode responsePath = mock(JsonNode.class);

        when(mapper.readTree(KAFKA_VALUE)).thenReturn(mockNode);
        when(mockNode.path(Constants.REQUEST_DATA)).thenReturn(requestData);
        when(requestData.path(Constants.METADATA)).thenReturn(metadata);
        when(metadata.path(Constants.POST_ID)).thenReturn(mock(JsonNode.class));
        when(metadata.path(Constants.POST_ID).asText()).thenReturn("post123");

        when(mockNode.path(Constants.RESPONSE_DATA)).thenReturn(responseData);
        when(responseData.path(Constants.RESPONSE_DATA_PATH)).thenReturn(responsePath);
        when(responsePath.path(Constants.IS_PROFANE)).thenReturn(mock(JsonNode.class));
        when(responsePath.path(Constants.IS_PROFANE).asBoolean(false)).thenReturn(true);

        when(mockNode.toString()).thenReturn(KAFKA_VALUE);

        // Simulate exception
        doThrow(new RuntimeException("DB error")).when(discussionRepository)
                .updateProfanityFieldsByDiscussionId(anyString(), anyString(), anyBoolean());

        profanityConsumer.checkTextContentIsProfane(consumerRecord);

        // Wait for async execution
        Thread.sleep(200);

        // You can't directly verify log output with Lombok's @Slf4j, but you can use a library like SystemOutRule,
        // or refactor to inject a logger. For now, just verify the repository was called and exception didn't propagate.
        verify(discussionRepository, atLeastOnce()).updateProfanityFieldsByDiscussionId(
                eq("post123"), eq(KAFKA_VALUE), eq(true)
        );
    }
}