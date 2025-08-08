package com.igot.cb.profanity.impl;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.igot.cb.discussion.service.impl.DiscussionServiceImpl;
import com.igot.cb.pores.util.CbServerProperties;
import com.igot.cb.pores.util.Constants;
import com.igot.cb.transactional.service.RequestHandlerServiceImpl;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.lenient;

@ExtendWith(MockitoExtension.class)
class ProfanityCheckServiceImplTest {

    @Mock
    private RequestHandlerServiceImpl requestHandlerService;

    @Mock
    private CbServerProperties cbServerProperties;

    @InjectMocks
    private ProfanityCheckServiceImpl profanityCheckService;

    @Test
    void testProcessProfanityCheck_HappyPath() throws Exception {
        String id = String.valueOf(UUID.randomUUID());
        ObjectNode node = JsonNodeFactory.instance.objectNode();
        node.put(Constants.DESCRIPTION, "desc");
        node.put(Constants.LANGUAGE_CODE, "en");
        when(cbServerProperties.getCbDiscussionApiKey()).thenReturn("api-key");
        when(cbServerProperties.getCbServiceRegistryBaseUrl()).thenReturn("http://base-url");
        when(cbServerProperties.getCbRegistryTextModerationApiPath()).thenReturn("moderation");
        Method m = ProfanityCheckServiceImpl.class.getDeclaredMethod("processProfanityCheck", String.class, ObjectNode.class);
        m.setAccessible(true);
        m.invoke(profanityCheckService, id, node);
        verify(requestHandlerService).fetchResultUsingPost(anyString(), anyMap(), anyMap());
    }

    @Test
    void testProcessProfanityCheck_MissingDescription() throws Exception {
        String id = String.valueOf(UUID.randomUUID());
        ObjectNode node = JsonNodeFactory.instance.objectNode();
        node.put(Constants.LANGUAGE_CODE, "en");
        lenient().when(cbServerProperties.getCbDiscussionApiKey()).thenReturn("api-key");
        lenient().when(cbServerProperties.getCbServiceRegistryBaseUrl()).thenReturn("http://base-url");
        lenient().when(cbServerProperties.getCbRegistryTextModerationApiPath()).thenReturn("moderation");
        Method m = ProfanityCheckServiceImpl.class.getDeclaredMethod("processProfanityCheck", String.class, ObjectNode.class);
        m.setAccessible(true);
        InvocationTargetException ex = assertThrows(InvocationTargetException.class, () -> m.invoke(profanityCheckService, id, node));
        assertTrue(ex.getCause() instanceof NullPointerException);
    }

    @Test
    void testProcessProfanityCheck_MissingLanguageCode() throws Exception {
        String id = String.valueOf(UUID.randomUUID());
        ObjectNode node = JsonNodeFactory.instance.objectNode();
        node.put(Constants.DESCRIPTION, "desc");
        lenient().when(cbServerProperties.getCbDiscussionApiKey()).thenReturn("api-key");
        lenient().when(cbServerProperties.getCbServiceRegistryBaseUrl()).thenReturn("http://base-url");
        lenient().when(cbServerProperties.getCbRegistryTextModerationApiPath()).thenReturn("moderation");
        Method m = ProfanityCheckServiceImpl.class.getDeclaredMethod("processProfanityCheck", String.class, ObjectNode.class);
        m.setAccessible(true);
        InvocationTargetException ex = assertThrows(InvocationTargetException.class, () -> m.invoke(profanityCheckService, id, node));
        assertTrue(ex.getCause() instanceof NullPointerException);
    }

    @Test
    void testProcessProfanityCheck_NullDiscussionDetailsNode() throws Exception {
        String id = String.valueOf(UUID.randomUUID());
        lenient().when(cbServerProperties.getCbDiscussionApiKey()).thenReturn("api-key");
        lenient().when(cbServerProperties.getCbServiceRegistryBaseUrl()).thenReturn("http://base-url");
        lenient().when(cbServerProperties.getCbRegistryTextModerationApiPath()).thenReturn("moderation");
        Method m = ProfanityCheckServiceImpl.class.getDeclaredMethod("processProfanityCheck", String.class, ObjectNode.class);
        m.setAccessible(true);
        InvocationTargetException ex = assertThrows(InvocationTargetException.class, () -> m.invoke(profanityCheckService, id, null));
        assertTrue(ex.getCause() instanceof NullPointerException);
    }

    @Test
    void testProcessProfanityCheck_NullId() throws Exception {
        ObjectNode node = JsonNodeFactory.instance.objectNode();
        node.put(Constants.DESCRIPTION, "desc");
        node.put(Constants.LANGUAGE_CODE, "en");
        when(cbServerProperties.getCbDiscussionApiKey()).thenReturn("api-key");
        when(cbServerProperties.getCbServiceRegistryBaseUrl()).thenReturn("http://base-url");
        when(cbServerProperties.getCbRegistryTextModerationApiPath()).thenReturn("moderation");
        Method m = ProfanityCheckServiceImpl.class.getDeclaredMethod("processProfanityCheck", String.class, ObjectNode.class);
        m.setAccessible(true);
        m.invoke(profanityCheckService, null, node);
        verify(requestHandlerService).fetchResultUsingPost(anyString(), anyMap(), anyMap());
    }
}
