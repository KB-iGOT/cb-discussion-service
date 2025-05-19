package com.igot.cb.producer;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.kafka.core.KafkaTemplate;


import static org.mockito.Mockito.*;

class ProducerTest {

    @InjectMocks
    private Producer producer;

    @Mock
    private KafkaTemplate<String, String> kafkaTemplate;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    void push_shouldSendMessageSuccessfully() {
        // given
        String topic = "test-topic";
        TestMessage message = new TestMessage("Alice", 25);

        // when
        producer.push(topic, message);

        // then
        verify(kafkaTemplate, times(1)).send(eq(topic), anyString());
    }

    // Simple POJO for test
    static class TestMessage {
        public String name;
        public int age;

        public TestMessage(String name, int age) {
            this.name = name;
            this.age = age;
        }
    }
}
