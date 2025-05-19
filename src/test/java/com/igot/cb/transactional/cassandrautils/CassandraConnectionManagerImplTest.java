package com.igot.cb.transactional.cassandrautils;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.igot.cb.pores.exceptions.CustomException;
import com.igot.cb.pores.util.Constants;
import com.igot.cb.pores.util.PropertiesCache;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.jupiter.MockitoExtension;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class CassandraConnectionManagerImplTest {

    @Mock
    PropertiesCache propertiesCache;

    @Mock
    CqlSession mockSession;

    @Mock
    Metadata metadata;

    @Mock
    Node mockNode;

    CassandraConnectionManagerImpl manager;

    @BeforeEach
    void setup() {
        MockitoAnnotations.openMocks(this);
    }

//    @Test
//    void testGetSession_existingSession() throws Exception {
//        try (MockedStatic<PropertiesCache> staticMock = mockStatic(PropertiesCache.class)) {
//            // Static mocking
//            staticMock.when(PropertiesCache::getInstance).thenReturn(propertiesCache);
//            when(propertiesCache.getProperty(Constants.CASSANDRA_CONFIG_HOST)).thenReturn("localhost");
//
//            manager = Mockito.spy(new CassandraConnectionManagerImpl());
//
//            // put session manually
//            Field sessionMapField = CassandraConnectionManagerImpl.class.getDeclaredField("cassandraSessionMap");
//            sessionMapField.setAccessible(true);
//            @SuppressWarnings("unchecked")
//            Map<String, CqlSession> sessionMap = (Map<String, CqlSession>) sessionMapField.get(null);
//            sessionMap.put("testkeyspace", mockSession);
//
//            when(mockSession.isClosed()).thenReturn(false);
//
//            CqlSession returnedSession = manager.getSession("testkeyspace");
//            assertEquals(mockSession, returnedSession);
//        }
//    }

//    @Test
//    void testGetSession_createsNewSession() throws Exception {
//        try (MockedStatic<PropertiesCache> staticMock = mockStatic(PropertiesCache.class)) {
//            staticMock.when(PropertiesCache::getInstance).thenReturn(propertiesCache);
//
//            when(propertiesCache.getProperty(Constants.CASSANDRA_CONFIG_HOST)).thenReturn("localhost");
//            mockCommonProperties(propertiesCache);
//
//            CassandraConnectionManagerImpl spy = Mockito.spy(new CassandraConnectionManagerImpl());
//
//            CqlSession mockCreatedSession = mock(CqlSession.class);
//            doReturn(mockCreatedSession).when(spy).getSession(anyString());
//
//            CqlSession session = spy.getSession("newkeyspace");
//            assertNotNull(session);
//        }
//    }

//    @Test
//    void testCreateCassandraConnectionWithKeySpaces_exception() {
//        try (MockedStatic<PropertiesCache> staticMock = mockStatic(PropertiesCache.class)) {
//            staticMock.when(PropertiesCache::getInstance).thenReturn(propertiesCache);
//
//            when(propertiesCache.getProperty(Constants.CASSANDRA_CONFIG_HOST)).thenReturn("127.0.0.1");
//
//            manager = new CassandraConnectionManagerImpl();
//
//            assertThrows(CustomException.class,
//                    () -> manager.getSession("invalid-keyspace"));
//        }
//    }

    @Test
    void testGetConsistencyLevel_valid() {
        try (MockedStatic<PropertiesCache> staticMock = mockStatic(PropertiesCache.class)) {
            staticMock.when(PropertiesCache::getInstance).thenReturn(propertiesCache);
            when(propertiesCache.readProperty(Constants.SUNBIRD_CASSANDRA_CONSISTENCY_LEVEL))
                    .thenReturn("LOCAL_QUORUM");

            ConsistencyLevel level = invokeGetConsistencyLevel();
            assertEquals(DefaultConsistencyLevel.LOCAL_QUORUM, level);
        }
    }

    @Test
    void testGetConsistencyLevel_invalid() {
        try (MockedStatic<PropertiesCache> staticMock = mockStatic(PropertiesCache.class)) {
            staticMock.when(PropertiesCache::getInstance).thenReturn(propertiesCache);
            when(propertiesCache.readProperty(Constants.SUNBIRD_CASSANDRA_CONSISTENCY_LEVEL))
                    .thenReturn("INVALID");

            ConsistencyLevel level = invokeGetConsistencyLevel();
            assertNull(level);
        }
    }

    @Test
    void testShutdownHook() {
        Thread thread = new CassandraConnectionManagerImpl.ResourceCleanUp();
        thread.run(); // manually invoke shutdown hook
    }

    // === Utility ===

    private void mockCommonProperties(PropertiesCache propertiesCache) {
        when(propertiesCache.getProperty(Constants.CORE_CONNECTIONS_PER_HOST_FOR_LOCAL)).thenReturn("1");
        when(propertiesCache.getProperty(Constants.CORE_CONNECTIONS_PER_HOST_FOR_REMOTE)).thenReturn("1");
        when(propertiesCache.getProperty(Constants.HEARTBEAT_INTERVAL)).thenReturn("5");
    }

    private ConsistencyLevel invokeGetConsistencyLevel() {
        try {
            Method method = CassandraConnectionManagerImpl.class.getDeclaredMethod("getConsistencyLevel");
            method.setAccessible(true);
            return (ConsistencyLevel) method.invoke(null);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
