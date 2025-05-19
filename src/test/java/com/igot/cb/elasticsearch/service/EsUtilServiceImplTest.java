package com.igot.cb.elasticsearch.service;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.Result;
import co.elastic.clients.elasticsearch.core.DeleteRequest;
import co.elastic.clients.elasticsearch.core.DeleteResponse;
import co.elastic.clients.elasticsearch.core.IndexRequest;
import co.elastic.clients.elasticsearch.indices.ElasticsearchIndicesClient;
import co.elastic.clients.elasticsearch.indices.RefreshRequest;
import co.elastic.clients.elasticsearch.indices.RefreshResponse;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.pores.elasticsearch.service.EsUtilServiceImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.slf4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;


class EsUtilServiceImplTest {

    private EsUtilServiceImpl esUtilService;
    private ElasticsearchClient elasticsearchClient;
    private ObjectMapper objectMapper;

    @Mock
    private ElasticsearchIndicesClient indicesClient;
    @Mock
    private DeleteResponse deleteResponse;

    @Mock
    private RefreshResponse refreshResponse;

    @Mock
    private Logger log;

    private final String testIndex = "test-index";
    private final String testId = "test-id";

    @BeforeEach
    void setUp() throws Exception {
        elasticsearchClient = mock(ElasticsearchClient.class);
        objectMapper = new ObjectMapper();
        esUtilService = new EsUtilServiceImpl(elasticsearchClient);
        when(elasticsearchClient.indices()).thenReturn(indicesClient);

        // Use reflection to inject ObjectMapper (simulates @Autowired)
        Field objectMapperField = EsUtilServiceImpl.class.getDeclaredField("objectMapper");
        objectMapperField.setAccessible(true);
        objectMapperField.set(esUtilService, objectMapper);

        // Clear static schema cache to ensure test isolation
        Field cacheField = EsUtilServiceImpl.class.getDeclaredField("schemaCache");
        cacheField.setAccessible(true);
        ((Map<?, ?>) cacheField.get(null)).clear();
    }

    @Test
    void testAddDocument_schemaFileNotFound() {
        String result = esUtilService.addDocument("index", "1", Map.of("key", "val"), "/nonexistent.json");
        assertNull(result);
    }

    @Test
    void testAddDocument_invalidJsonSchema() {
        String result = esUtilService.addDocument("index", "1", Map.of("key", "val"), "/invalid-schema.json");
        assertNull(result);
    }

    @Test
    void deleteDocument_Success() throws Exception {
        // Mock DeleteResponse
        DeleteResponse deleteResponse = mock(DeleteResponse.class);
        when(deleteResponse.result()).thenReturn(Result.Deleted);
        when(elasticsearchClient.delete(any(DeleteRequest.class))).thenReturn(deleteResponse);

        // Mock RefreshResponse - THIS IS WHAT WAS MISSING
        RefreshResponse refreshResponse = mock(RefreshResponse.class);

        // Execute
        esUtilService.deleteDocument(testId, testIndex);
    }


}
