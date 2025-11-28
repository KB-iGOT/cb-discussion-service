package com.igot.cb.pores.util;

import com.networknt.schema.JsonSchema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import org.igot.common.PropertiesCache;

@ExtendWith(MockitoExtension.class)
class JsonSchemaCacheTest {

    @Mock
    PropertiesCache propertiesCache;

    private JsonSchemaCache jsonSchemaCache;

    @BeforeEach
    void setUp() {
        jsonSchemaCache = new JsonSchemaCache(propertiesCache);
    }

    @Test
    void testGetSchema_CacheMiss_LoadsSchema() {
        String schemaKey = "test.schema.key";
        String schemaPath = "schema/test-schema.json"; // put a valid schema in src/test/resources/schema/test-schema.json

        when(propertiesCache.getProperty(schemaKey)).thenReturn(schemaPath);
        JsonSchema schema = jsonSchemaCache.getSchema(schemaKey);
        assertNotNull(schema);

        // Revalidate from cache
        JsonSchema cachedSchema = jsonSchemaCache.getSchema(schemaKey);
        assertSame(schema, cachedSchema);
    }

    @Test
    void testGetSchema_CacheHit_ReturnsCachedSchema() {
        String schemaKey = "test.key.hit";
        String schemaPath = "schema/test-schema.json";

        when(propertiesCache.getProperty(schemaKey)).thenReturn(schemaPath);

        // First load (cache miss)
        JsonSchema firstLoad = jsonSchemaCache.getSchema(schemaKey);
        assertNotNull(firstLoad);

        // Second load (cache hit)
        JsonSchema secondLoad = jsonSchemaCache.getSchema(schemaKey);
        assertSame(firstLoad, secondLoad); // same object from cache
    }

    @Test
    void testLoadSchema_WhenFileNotFound_ThrowsException() {
        String schemaKey = "invalid.schema.key";
        String invalidPath = "schema/non-existent.json";

        when(propertiesCache.getProperty(schemaKey)).thenReturn(invalidPath);

        RuntimeException ex = assertThrows(RuntimeException.class, () ->
                jsonSchemaCache.getSchema(schemaKey));
        assertTrue(ex.getMessage().contains("Failed to load JSON schema"));
    }
}
