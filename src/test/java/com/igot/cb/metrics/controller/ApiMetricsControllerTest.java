package com.igot.cb.metrics.controller;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;

import com.igot.cb.metrics.service.ApiMetricsTracker;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.igot.common.ApiResponse;

@ExtendWith(MockitoExtension.class)
class ApiMetricsControllerTest {

    @InjectMocks
    private ApiMetricsController controller;

    @BeforeEach
    void setup() {
        // No-op for now
    }

    @Test
    void testGetApiMetrics() {
        String apiEndpoint = "/test";
        ApiMetricsTracker.ApiMetricsResponse mockResponse = new ApiMetricsTracker.ApiMetricsResponse();

        try (MockedStatic<ApiMetricsTracker> mockedTracker = Mockito.mockStatic(ApiMetricsTracker.class)) {
            mockedTracker.when(() -> ApiMetricsTracker.getApiMetrics(apiEndpoint)).thenReturn(mockResponse);

            ApiMetricsTracker.ApiMetricsResponse response = controller.getApiMetrics(apiEndpoint);

            assertNotNull(response);
        }
    }

    @Test
    void testEnableTracking() {
        try (
                MockedStatic<ApiMetricsTracker> mockedTracker = Mockito.mockStatic(ApiMetricsTracker.class);
                MockedStatic<ApiResponse> mockedUtil = Mockito.mockStatic(ApiResponse.class)
        ) {
            ApiResponse mockResponse = new ApiResponse();
            mockedUtil.when(() -> ApiResponse.createDefaultResponse("/api/metrics/enableTracking")).thenReturn(mockResponse);

            ApiResponse response = controller.enableTracking();

            assertNotNull(response);
            mockedTracker.verify(ApiMetricsTracker::enableTracking);
        }
    }

    @Test
    void testDisableTracking() {
        try (
                MockedStatic<ApiMetricsTracker> mockedTracker = Mockito.mockStatic(ApiMetricsTracker.class);
                MockedStatic<ApiResponse> mockedUtil = Mockito.mockStatic(ApiResponse.class)
        ) {
            ApiResponse mockResponse = new ApiResponse();
            mockedUtil.when(() -> ApiResponse.createDefaultResponse("/api/metrics/disableTracking")).thenReturn(mockResponse);

            ApiResponse response = controller.disableTracking();

            assertNotNull(response);
            mockedTracker.verify(ApiMetricsTracker::disableTracking);
        }
    }
}
