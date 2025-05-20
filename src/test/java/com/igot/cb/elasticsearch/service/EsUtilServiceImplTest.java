package com.igot.cb.elasticsearch.service;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.Result;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch.core.*;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.elasticsearch.core.search.HitsMetadata;
import co.elastic.clients.elasticsearch.core.search.TotalHits;
import co.elastic.clients.elasticsearch.core.search.TotalHitsRelation;
import co.elastic.clients.json.JsonData;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.pores.elasticsearch.dto.SearchCriteria;
import com.igot.cb.pores.elasticsearch.dto.SearchResult;
import com.igot.cb.pores.elasticsearch.service.EsUtilServiceImpl;
import com.igot.cb.pores.exceptions.CustomException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.*;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.http.HttpStatus;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@MockitoSettings(strictness = Strictness.LENIENT)
@ExtendWith(MockitoExtension.class)
class EsUtilServiceImplTest {


    private ElasticsearchClient elasticsearchClient= Mockito.mock(ElasticsearchClient.class);

    @Mock
    private ObjectMapper objectMapper;

    @InjectMocks
    private EsUtilServiceImpl esUtilService;

    private SearchCriteria sampleCriteria;
    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        sampleCriteria = new SearchCriteria();
        sampleCriteria.setPageNumber(0);
        sampleCriteria.setPageSize(2);
        sampleCriteria.setSearchString("example");
        sampleCriteria.setRequestedFields(List.of("title", "description"));
        sampleCriteria.setFacets(List.of("category"));
        sampleCriteria.setOrderBy("title");
        sampleCriteria.setOrderDirection("asc");

        Map<String, Object> filters = new HashMap<>();
        filters.put("status", List.of("active", "pending"));
        filters.put("rating", Map.of("gte", JsonData.of(4)));
        sampleCriteria.setFilterCriteriaMap((HashMap<String, Object>) filters);
    }


    @Test
    void testReadSchema_throwsCustomException() {
        String index = "test-index";
        String schemaPath = "schema.json";
        CustomException ex = assertThrows(CustomException.class, () -> {
            esUtilService.searchDocuments(index, sampleCriteria, schemaPath);
        });

        assertEquals("argument \"src\" is null", ex.getMessage());
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, ex.getHttpStatusCode());
    }

    @Test
    void saveAll_shouldReturnBulkResponse_whenSuccess() throws IOException {
        // Arrange
        String esIndexName = "test-index";
        String testId = "123";
        Map<String, Object> entityMap = Map.of("id", testId, "name", "Test");

        JsonNode jsonNode = mock(JsonNode.class);
        JsonNode idNode = mock(JsonNode.class);

        List<JsonNode> entities = List.of(jsonNode);

        when(jsonNode.get("id")).thenReturn(idNode);
        when(idNode.asText()).thenReturn(testId);
        when(objectMapper.convertValue(any(JsonNode.class), eq(Map.class))).thenReturn(entityMap);

        BulkResponse mockResponse = mock(BulkResponse.class);
        lenient().when(elasticsearchClient.bulk((BulkRequest) any())).thenReturn(mockResponse);
        // Act
        BulkResponse response = esUtilService.saveAll(esIndexName, entities);

        // Assert
        assertNotNull(response);
    }

    @Test
    void testDeleteDocumentsByCriteria_noHits() throws IOException {
        // Given
        Query query = Query.of(q -> q.matchAll(m -> m));

        TotalHits totalHits = new TotalHits.Builder()
                .value(0L)
                .relation(TotalHitsRelation.Eq)
                .build();

        HitsMetadata<Object> hitsMetadata = new HitsMetadata.Builder<>()
                .total(totalHits)
                .hits(Collections.emptyList())
                .build();

        SearchResponse<Object> searchResponse = new SearchResponse.Builder<Object>()
                .took(10)
                .timedOut(false)
                .shards(s -> s.total(1).successful(1).failed(0).skipped(0))
                .hits(hitsMetadata)
                .build();

        when(elasticsearchClient.search(any(SearchRequest.class), eq(Object.class)))
                .thenReturn(searchResponse);

        // When
        esUtilService.deleteDocumentsByCriteria("test-index", query);

        // Then
        verify(elasticsearchClient, never()).bulk(any(BulkRequest.class));
    }


    @Test
    void testDeleteDocumentsByCriteria_exceptionThrown() throws IOException {
        // Arrange
        Query query = Query.of(q -> q.matchAll(m -> m));
        when(elasticsearchClient.search(any(SearchRequest.class), eq(Object.class))).thenThrow(new IOException("search failed"));

        // Act
        esUtilService.deleteDocumentsByCriteria("test-index", query);

        // Assert
        verify(elasticsearchClient, never()).bulk(any(BulkRequest.class));
    }

    @Test
    void testAddDocument_schemaFileNotFound() {
        String result = esUtilService.addDocument("index", "1", Map.of("key", "val"), "/nonexistent.json");
        assertNull(result);
    }

    @Test
    void testAddDocument_successfullyIndexed() throws Exception {
        // Given
        String indexName = "test-index";
        String documentId = "123";
        String jsonFilePath = "/schema.json";

        Map<String, Object> schemaMap = Map.of(
                "field1", "string",
                "field2", "integer"
        );

        Map<String, Object> inputDocument = new HashMap<>();
        inputDocument.put("field1", "value1");
        inputDocument.put("field2", 123);
        inputDocument.put("extraField", "shouldBeRemoved");

        // Mock ObjectMapper behavior
        when(objectMapper.readValue(
                any(InputStream.class),
                ArgumentMatchers.<TypeReference<Map<String, Object>>>any()
        )).thenReturn(schemaMap);

        // Mock index response
        IndexResponse mockResponse = mock(IndexResponse.class);
        when(mockResponse.result()).thenReturn(Result.Created);
        when(elasticsearchClient.index(any(IndexRequest.class))).thenReturn(mockResponse);
        // When
        String result = esUtilService.addDocument(indexName, documentId, inputDocument, jsonFilePath);

        // Then
        assertNotNull(result); // ensure the response isn't null
        assertTrue(result.contains("Successfully indexed"));
        verify(elasticsearchClient, times(1)).index((IndexRequest<Object>) any());
    }

    @Test
    void testUpdated_successfullyUpdated() throws Exception {
        // Given
        String indexName = "test-index";
        String documentId = "123";
        String jsonFilePath = "/schema.json";

        Map<String, Object> schemaMap = Map.of(
                "field1", "string",
                "field2", "integer"
        );

        Map<String, Object> inputDocument = new HashMap<>();
        inputDocument.put("field1", "value1");
        inputDocument.put("field2", 123);
        inputDocument.put("extraField", "shouldBeRemoved");

        // Mock ObjectMapper behavior
        when(objectMapper.readValue(
                any(InputStream.class),
                ArgumentMatchers.<TypeReference<Map<String, Object>>>any()
        )).thenReturn(schemaMap);

        // Mock index response
        IndexResponse mockResponse = mock(IndexResponse.class);
        when(mockResponse.result()).thenReturn(Result.Created);
        when(elasticsearchClient.index(any(IndexRequest.class))).thenReturn(mockResponse);
        // When
        String result = esUtilService.updateDocument(indexName, documentId, inputDocument, jsonFilePath);

        // Then
        assertNotNull(result); // ensure the response isn't null
        assertTrue(result.contains("created"));
        verify(elasticsearchClient, times(1)).index((IndexRequest<Object>) any());
    }

    @Test
    void searchDocuments_Success() throws IOException {
        // Test data setup
        String esIndexName = "test-index";
        SearchCriteria searchCriteria = new SearchCriteria();
        searchCriteria.setPageNumber(0);
        searchCriteria.setPageSize(10);
        String jsonFilePath = "/test-schema.json";

        // Mock SearchResponse
        SearchResponse<Object> mockSearchResponse = mock(SearchResponse.class);
        HitsMetadata<Object> hitsMetadata = mock(HitsMetadata.class);
        TotalHits totalHits = mock(TotalHits.class);
        List<Hit<Object>> hits = new ArrayList<>();

        // Configure mock responses
        when(totalHits.value()).thenReturn(1L);
        when(hitsMetadata.total()).thenReturn(totalHits);
        when(hitsMetadata.hits()).thenReturn(hits);
        when(mockSearchResponse.hits()).thenReturn(hitsMetadata);

        // Mock elasticsearch client search method
        when(elasticsearchClient.search(any(SearchRequest.class), eq(Object.class)))
                .thenReturn(mockSearchResponse);

        // Execute
        SearchResult result = esUtilService.searchDocuments(esIndexName, searchCriteria, jsonFilePath);

        // Verify
        assertNotNull(result);
        assertEquals(1L, result.getTotalCount());
        assertNotNull(result.getData());
        assertNotNull(result.getFacets());
    }
}
