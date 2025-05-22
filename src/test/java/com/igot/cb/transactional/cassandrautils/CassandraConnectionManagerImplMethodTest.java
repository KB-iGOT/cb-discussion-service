package com.igot.cb.transactional.cassandrautils;

import com.datastax.oss.driver.api.core.*;
import com.igot.cb.pores.exceptions.CustomException;
import com.igot.cb.pores.util.Constants;
import com.igot.cb.pores.util.PropertiesCache;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.*;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class CassandraConnectionManagerImplMethodTest {

    private CassandraConnectionManagerImpl cassandraConnectionManager;

    private MockedStatic<CqlSession> sessionStaticMock;
    private MockedStatic<PropertiesCache> propertiesCacheStaticMock;
    private MockedStatic<StringUtils> stringUtilsMock;

    @BeforeEach
    void setup() {
        MockitoAnnotations.openMocks(this);
        cassandraConnectionManager = new CassandraConnectionManagerImpl();
        sessionStaticMock = mockStatic(CqlSession.class);
        propertiesCacheStaticMock = mockStatic(PropertiesCache.class);
        stringUtilsMock = mockStatic(StringUtils.class);
    }

    @AfterEach
    void tearDown() {
        sessionStaticMock.close();
        propertiesCacheStaticMock.close();
        stringUtilsMock.close();
    }

    @Test
    void testGetConsistencyLevel_shouldReturnDefaultConsistencyLevel() {
        PropertiesCache cache = mock(PropertiesCache.class);
        propertiesCacheStaticMock.when(PropertiesCache::getInstance).thenReturn(cache);
        when(cache.readProperty(Constants.SUNBIRD_CASSANDRA_CONSISTENCY_LEVEL)).thenReturn("ONE");

        Method method = ReflectionUtils.findMethod(CassandraConnectionManagerImpl.class, "getConsistencyLevel");
        method.setAccessible(true);
        ConsistencyLevel level = (ConsistencyLevel) ReflectionUtils.invokeMethod(method, null);

        assertEquals(DefaultConsistencyLevel.ONE, level);
    }

    @Test
    void testGetConsistencyLevel_withInvalidValue_shouldReturnNull() {
        PropertiesCache cache = mock(PropertiesCache.class);
        propertiesCacheStaticMock.when(PropertiesCache::getInstance).thenReturn(cache);
        when(cache.readProperty(Constants.SUNBIRD_CASSANDRA_CONSISTENCY_LEVEL)).thenReturn("INVALID");

        Method method = ReflectionUtils.findMethod(CassandraConnectionManagerImpl.class, "getConsistencyLevel");
        method.setAccessible(true);
        ConsistencyLevel level = (ConsistencyLevel) ReflectionUtils.invokeMethod(method, null);

        assertNull(level);
    }

    @Test
    void testGetSession_withBlankHost_shouldThrowException() {
        PropertiesCache propertiesCache = mock(PropertiesCache.class);
        propertiesCacheStaticMock.when(PropertiesCache::getInstance).thenReturn(propertiesCache);

        when(propertiesCache.getProperty(Constants.CASSANDRA_CONFIG_HOST)).thenReturn("");
        sessionStaticMock.when(() -> StringUtils.isBlank("")).thenReturn(true);

        CustomException exception = assertThrows(CustomException.class, () ->
                cassandraConnectionManager.getSession("testKeyspace"));

        assertEquals("Cassandra host is not configured", exception.getMessage());
    }
}