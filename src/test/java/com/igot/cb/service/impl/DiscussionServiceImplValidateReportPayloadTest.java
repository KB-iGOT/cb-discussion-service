package com.igot.cb.service.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.discussion.repository.CommunityEngagementRepository;
import com.igot.cb.discussion.repository.DiscussionAnswerPostReplyRepository;
import com.igot.cb.discussion.repository.DiscussionRepository;
import com.igot.cb.discussion.service.impl.DiscussionServiceImpl;
import com.igot.cb.notification.HelperMethodService;
import com.igot.cb.notification.NotificationTriggerService;
import com.igot.cb.pores.cache.CacheService;
import com.igot.cb.pores.elasticsearch.dto.SearchResult;
import com.igot.cb.pores.elasticsearch.service.EsUtilService;
import com.igot.cb.pores.util.*;
import com.igot.cb.producer.Producer;
import org.igot.common.auth.AccessTokenValidator;
import org.igot.common.cassandra.CassandraOperation;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.RedisTemplate;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.MockitoAnnotations.openMocks;

@ExtendWith(MockitoExtension.class)
class DiscussionServiceImplValidateReportPayloadTest {

    @Mock
    private PayloadValidation payloadValidation;
    @Mock
    private DiscussionRepository discussionRepository;
    @Mock
    private CacheService cacheService;
    @Mock
    private EsUtilService esUtilService;
    @Mock
    private CbServerProperties cbServerProperties;
    @Mock
    private RedisTemplate<String, SearchResult> redisTemplate;
    @Mock
    private ObjectMapper objectMapper;
    @Mock
    private CassandraOperation cassandraOperation;
    @Mock
    private AccessTokenValidator accessTokenValidator;
    @Mock
    private CommunityEngagementRepository communityEngagementRepository;
    @Mock
    private Producer producer;
    @Mock
    private DiscussionAnswerPostReplyRepository discussionAnswerPostReplyRepository;
    @Mock
    private NotificationTriggerService notificationTriggerService;
    @Mock
    private HelperMethodService helperMethodService;
    @Mock
    private DiscussionServiceUtil discussionServiceUtil;

    private DiscussionServiceImpl service;
    private Method validateReportPayloadMethod;

    @BeforeEach
    void setUp() throws Exception {
        openMocks(this);
        service = new DiscussionServiceImpl(
                payloadValidation,
                discussionRepository,
                cacheService,
                esUtilService,
                cbServerProperties,
                redisTemplate,
                objectMapper,
                cassandraOperation,
                accessTokenValidator,
                communityEngagementRepository,
                producer,
                discussionAnswerPostReplyRepository,
                notificationTriggerService,
                helperMethodService,
                discussionServiceUtil
        );
        validateReportPayloadMethod = DiscussionServiceImpl.class
                .getDeclaredMethod("validateReportPayload", Map.class);
        validateReportPayloadMethod.setAccessible(true);
    }

    @Test
    void testEmptyMap_returnsEmptyString() throws Exception {
        Map<String, Object> payload = new HashMap<>();
        String result = (String) validateReportPayloadMethod.invoke(service, payload);
        assertEquals("", result);
    }

    @Test
    void testBlankDiscussionId_addsError() throws Exception {
        Map<String, Object> payload = new HashMap<>();
        payload.put(Constants.DISCUSSION_ID, ""); // blank
        String result = (String) validateReportPayloadMethod.invoke(service, payload);
        assertTrue(result.contains(Constants.DISCUSSION_ID));
    }

    @Test
    void testBlankType_addsError() throws Exception {
        Map<String, Object> payload = new HashMap<>();
        payload.put(Constants.TYPE, ""); // blank
        String result = (String) validateReportPayloadMethod.invoke(service, payload);
        assertTrue(result.contains(Constants.TYPE));
    }

    @Test
    void testReportedReason_notList_addsError() throws Exception {
        Map<String, Object> payload = new HashMap<>();
        payload.put(Constants.REPORTED_REASON, "notAList");
        String result = (String) validateReportPayloadMethod.invoke(service, payload);
        assertTrue(result.contains(Constants.REPORTED_REASON));
    }

    @Test
    void testReportedReason_listEmpty_addsError() throws Exception {
        Map<String, Object> payload = new HashMap<>();
        payload.put(Constants.REPORTED_REASON, new ArrayList<String>());
        String result = (String) validateReportPayloadMethod.invoke(service, payload);
        assertTrue(result.contains(Constants.REPORTED_REASON));
    }

    @Test
    void testReportedReason_containsOthers_withoutOtherReason_addsError() throws Exception {
        Map<String, Object> payload = new HashMap<>();
        payload.put(Constants.REPORTED_REASON, Arrays.asList(Constants.OTHERS));
        String result = (String) validateReportPayloadMethod.invoke(service, payload);
        assertTrue(result.contains(Constants.OTHER_REASON));
    }

    @Test
    void testReportedReason_containsOthers_withBlankOtherReason_addsError() throws Exception {
        Map<String, Object> payload = new HashMap<>();
        payload.put(Constants.REPORTED_REASON, Arrays.asList(Constants.OTHERS));
        payload.put(Constants.OTHER_REASON, ""); // blank
        String result = (String) validateReportPayloadMethod.invoke(service, payload);
        assertTrue(result.contains(Constants.OTHER_REASON));
    }

    @Test
    void testReportedReason_containsOthers_withValidOtherReason_noError() throws Exception {
        Map<String, Object> payload = new HashMap<>();
        payload.put(Constants.REPORTED_REASON, Arrays.asList(Constants.OTHERS));
        payload.put(Constants.OTHER_REASON, "valid reason");
        String result = (String) validateReportPayloadMethod.invoke(service, payload);
        assertEquals("", result);
    }

    @Test
    void testMultipleErrors_allAdded() throws Exception {
        Map<String, Object> payload = new HashMap<>();
        payload.put(Constants.DISCUSSION_ID, "");
        payload.put(Constants.TYPE, "");
        payload.put(Constants.REPORTED_REASON, "notAList");
        String result = (String) validateReportPayloadMethod.invoke(service, payload);

        assertTrue(result.contains(Constants.DISCUSSION_ID));
        assertTrue(result.contains(Constants.TYPE));
        assertTrue(result.contains(Constants.REPORTED_REASON));
    }
}
