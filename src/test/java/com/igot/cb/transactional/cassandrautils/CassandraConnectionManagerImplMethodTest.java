package com.igot.cb.transactional.cassandrautils;

import com.datastax.oss.driver.api.core.*;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
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
    @InjectMocks
    private CassandraConnectionManagerImpl cassandraConnectionManager;

    @Mock
    private CqlSession mockSession;

    @Mock
    private Metadata mockMetadata;

    @Mock
    private Node mockNode;

    @Mock
    private EndPoint mockEndPoint;

    @Mock
    private CqlSessionBuilder mockBuilder;

    @Mock
    private DriverConfigLoader mockLoader;

    @Mock
    private PropertiesCache propertiesCache;

    private MockedStatic<CqlSession> sessionStaticMock;
    private MockedStatic<PropertiesCache> propertiesCacheStaticMock;
    private MockedStatic<StringUtils> stringUtilsMock;

    @BeforeEach
    void setup() {
        MockitoAnnotations.openMocks(this);

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
//    @Test
//    void testGetSession_withValidKeyspace_shouldCreateAndReturnSession() {
//        // Setup keyspace
//        String keyspaceName = "testkeyspace";
//
//        // Mock properties
//        propertiesCacheStaticMock.when(PropertiesCache::getInstance).thenReturn(propertiesCache);
//        when(propertiesCache.getProperty(Constants.CASSANDRA_CONFIG_HOST)).thenReturn("127.0.0.1");
//        when(propertiesCache.getProperty(Constants.CORE_CONNECTIONS_PER_HOST_FOR_LOCAL)).thenReturn("1");
//        when(propertiesCache.getProperty(Constants.CORE_CONNECTIONS_PER_HOST_FOR_REMOTE)).thenReturn("1");
//        when(propertiesCache.getProperty(Constants.HEARTBEAT_INTERVAL)).thenReturn("1");
//        when(propertiesCache.readProperty(Constants.SUNBIRD_CASSANDRA_CONSISTENCY_LEVEL)).thenReturn("ONE");
//
//        // Mock StringUtils
//        stringUtilsMock.when(() -> StringUtils.isBlank("127.0.0.1")).thenReturn(false);
//        stringUtilsMock.when(() -> StringUtils.isNotBlank(keyspaceName)).thenReturn(true);
//
//        // Mock session and metadata
//        when(mockSession.getMetadata()).thenReturn(mockMetadata);
//        when(mockMetadata.getClusterName()).thenReturn(Optional.of("TestCluster"));
//        when(mockMetadata.getNodes()).thenReturn(Map.of(UUID.randomUUID(), mockNode));
//        when(mockNode.getDatacenter()).thenReturn("datacenter1");
//        when(mockNode.getEndPoint()).thenReturn(mockEndPoint);
//        when(mockNode.getRack()).thenReturn("rack1");
//
//        // Mock builder chain
//        sessionStaticMock.when(CqlSession::builder).thenReturn(mockBuilder);
//        when(mockBuilder.withKeyspace((CqlIdentifier) argThat(id -> id != null && id.equals("testkeyspace"))))
//                .thenReturn(mockBuilder);
//        //when(mockBuilder.withKeyspace(CqlIdentifier.fromCql(keyspaceName))).thenReturn(mockBuilder);
//        when(mockBuilder.withConfigLoader(any())).thenReturn(mockBuilder);
//        when(mockBuilder.addContactPoints(any())).thenReturn(mockBuilder); // ✅ FIXED LINE
//        when(mockBuilder.withLocalDatacenter(any())).thenReturn(mockBuilder);
//        when(mockBuilder.build()).thenReturn(mockSession);
//
//        // Call method under test
//        CqlSession session = cassandraConnectionManager.getSession(keyspaceName);
//
//        // Assert
//        assertNotNull(session);
//        assertEquals(mockSession, session);
//    }
    @Test
    void testGetConsistencyLevel_shouldReturnDefaultConsistencyLevel() {
        PropertiesCache cache = mock(PropertiesCache.class);
        propertiesCacheStaticMock.when(PropertiesCache::getInstance).thenReturn(cache);
        when(cache.readProperty(Constants.SUNBIRD_CASSANDRA_CONSISTENCY_LEVEL)).thenReturn("ONE");

        // Invoke private method using reflection
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