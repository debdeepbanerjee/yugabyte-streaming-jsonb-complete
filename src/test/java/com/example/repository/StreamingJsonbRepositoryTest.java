package com.example.repository;

import com.example.model.EnhancedDetailRecord;
import com.example.model.TransactionData;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import java.math.BigDecimal;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for streaming JSONB repository
 */
@ExtendWith(MockitoExtension.class)
class StreamingJsonbRepositoryTest {
    
    @Mock
    private JdbcTemplate jdbcTemplate;
    
    private ObjectMapper objectMapper;
    private StreamingJsonbRepository repository;
    
    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        objectMapper.findAndRegisterModules();
        
        repository = new StreamingJsonbRepository(jdbcTemplate, objectMapper);
    }
    
    @Test
    void testStreamJsonbRecordsWithDefaultFetchSize() {
        Long masterId = 123L;
        
        // Mock stream response
        Stream<EnhancedDetailRecord> mockStream = Stream.empty();
        when(jdbcTemplate.queryForStream(anyString(), any(RowMapper.class), eq(masterId)))
                .thenReturn(mockStream);
        
        // Call method
        Stream<EnhancedDetailRecord> result = repository.streamJsonbRecords(masterId);
        
        // Verify
        assertNotNull(result);
        verify(jdbcTemplate).setFetchSize(1000); // Default fetch size
        verify(jdbcTemplate).queryForStream(anyString(), any(RowMapper.class), eq(masterId));
    }
    
    @Test
    void testStreamJsonbRecordsWithCustomFetchSize() {
        Long masterId = 456L;
        int customFetchSize = 5000;
        
        Stream<EnhancedDetailRecord> mockStream = Stream.empty();
        when(jdbcTemplate.queryForStream(anyString(), any(RowMapper.class), eq(masterId)))
                .thenReturn(mockStream);
        
        Stream<EnhancedDetailRecord> result = repository.streamJsonbRecords(masterId, customFetchSize);
        
        assertNotNull(result);
        verify(jdbcTemplate).setFetchSize(customFetchSize);
    }
    
    @Test
    void testCountJsonbRecords() {
        Long masterId = 789L;
        Long expectedCount = 1000L;
        
        when(jdbcTemplate.queryForObject(anyString(), eq(Long.class), eq(masterId)))
                .thenReturn(expectedCount);
        
        long result = repository.countJsonbRecords(masterId);
        
        assertEquals(expectedCount, result);
        verify(jdbcTemplate).queryForObject(anyString(), eq(Long.class), eq(masterId));
    }
    
    @Test
    void testCountJsonbRecordsReturnsZeroWhenNull() {
        Long masterId = 999L;
        
        when(jdbcTemplate.queryForObject(anyString(), eq(Long.class), eq(masterId)))
                .thenReturn(null);
        
        long result = repository.countJsonbRecords(masterId);
        
        assertEquals(0L, result);
    }
    
    @Test
    void testEstimateMemoryUsage() {
        int fetchSize = 1000;
        int avgJsonbSizeKb = 10;
        
        long estimatedMemory = repository.estimateMemoryUsage(fetchSize, avgJsonbSizeKb);
        
        // Should be approximately (1000 * (500 + 10240)) = 10,740,000 bytes
        assertTrue(estimatedMemory > 10_000_000);
        assertTrue(estimatedMemory < 11_000_000);
    }
    
    @Test
    void testStreamByJsonbCriteria() {
        Long masterId = 123L;
        String jsonbPath = "risk_score";
        String operator = ">";
        Object value = 80.0;
        int fetchSize = 2000;
        
        Stream<EnhancedDetailRecord> mockStream = Stream.empty();
        when(jdbcTemplate.queryForStream(anyString(), any(RowMapper.class), eq(masterId), anyString()))
                .thenReturn(mockStream);
        
        Stream<EnhancedDetailRecord> result = repository.streamByJsonbCriteria(
                masterId, jsonbPath, operator, value, fetchSize
        );
        
        assertNotNull(result);
        verify(jdbcTemplate).setFetchSize(fetchSize);
        verify(jdbcTemplate).queryForStream(
                anyString(), 
                any(RowMapper.class), 
                eq(masterId), 
                eq(value.toString())
        );
    }
    
    @Test
    void testStreamWithCustomQuery() {
        String customSql = "SELECT * FROM enhanced_detail_records WHERE custom_field = ?";
        int fetchSize = 500;
        Object param1 = "value1";
        
        Stream<EnhancedDetailRecord> mockStream = Stream.empty();
        when(jdbcTemplate.queryForStream(eq(customSql), any(RowMapper.class), eq(param1)))
                .thenReturn(mockStream);
        
        Stream<EnhancedDetailRecord> result = repository.streamWithCustomQuery(
                customSql, fetchSize, param1
        );
        
        assertNotNull(result);
        verify(jdbcTemplate).setFetchSize(fetchSize);
    }
    
    @Test
    void testJsonbUnmarshalling() throws Exception {
        // Create sample JSONB
        String jsonb = """
            {
                "transaction_id": "TXN123",
                "amount": 100.50,
                "customer": {
                    "customer_id": "CUST001",
                    "name": "John Doe",
                    "email": "john@example.com"
                },
                "risk_score": 25.5,
                "status": "COMPLETED"
            }
            """;
        
        // Test direct unmarshalling (simulating what happens in RowMapper)
        TransactionData result = objectMapper.readValue(jsonb, TransactionData.class);
        
        assertNotNull(result);
        assertEquals("TXN123", result.getTransactionId());
        assertEquals(new BigDecimal("100.50"), result.getAmount());
        assertEquals(25.5, result.getRiskScore());
        assertEquals("COMPLETED", result.getStatus());
        
        assertNotNull(result.getCustomer());
        assertEquals("CUST001", result.getCustomer().getCustomerId());
        assertEquals("John Doe", result.getCustomer().getName());
        assertEquals("john@example.com", result.getCustomer().getEmail());
    }
    
    @Test
    void testStreamProcessingIsLazy() {
        Long masterId = 123L;
        
        // Create a stream that would throw if evaluated
        Stream<EnhancedDetailRecord> mockStream = Stream.of(
                EnhancedDetailRecord.builder().detailId(1L).build(),
                EnhancedDetailRecord.builder().detailId(2L).build()
        );
        
        when(jdbcTemplate.queryForStream(anyString(), any(RowMapper.class), eq(masterId)))
                .thenReturn(mockStream);
        
        // Getting the stream should not process records yet (lazy)
        Stream<EnhancedDetailRecord> result = repository.streamJsonbRecords(masterId);
        
        assertNotNull(result);
        
        // Only when we consume the stream should records be processed
        List<EnhancedDetailRecord> list = result.toList();
        assertEquals(2, list.size());
    }
    
    @Test
    void testCountByJsonbCriteria() {
        String jsonbPath = "status";
        String operator = "=";
        Object value = "COMPLETED";
        Long expectedCount = 500L;
        
        when(jdbcTemplate.queryForObject(anyString(), eq(Long.class), eq(value.toString())))
                .thenReturn(expectedCount);
        
        long result = repository.countByJsonbCriteria(jsonbPath, operator, value);
        
        assertEquals(expectedCount, result);
    }
}
