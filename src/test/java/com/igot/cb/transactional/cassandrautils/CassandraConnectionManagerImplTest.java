package com.igot.cb.transactional.cassandrautils;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.igot.cb.pores.util.Constants;
import com.igot.cb.pores.util.PropertiesCache;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.jupiter.MockitoExtension;

import java.lang.reflect.Method;

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
