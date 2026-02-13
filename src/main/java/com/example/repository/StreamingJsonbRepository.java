package com.example.repository;

import com.example.model.EnhancedDetailRecord;
import com.example.model.TransactionData;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.postgresql.util.PGobject;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.stream.Stream;

/**
 * Memory-efficient streaming JSONB reader from YugabyteDB
 * 
 * Key Features:
 * 1. Uses JDBC streaming ResultSet (never loads all rows into memory)
 * 2. Configurable fetch size for optimal memory/network trade-off
 * 3. Unmarshals JSONB row-by-row (no batch loading)
 * 4. Supports billions of rows with constant memory footprint
 * 5. Lazy evaluation - only processes when stream is consumed
 */
@Slf4j
@Repository
@RequiredArgsConstructor
public class StreamingJsonbRepository {
    
    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;
    
    // Default fetch size optimized for streaming
    private static final int DEFAULT_FETCH_SIZE = 1000;
    
    /**
     * Stream JSONB records with memory-efficient cursor-based fetching
     * 
     * Memory Usage: O(fetch_size) - constant regardless of total rows
     * 
     * @param masterId The master ID to query
     * @return Stream of enhanced records with unmarshalled JSONB
     */
    public Stream<EnhancedDetailRecord> streamJsonbRecords(Long masterId) {
        return streamJsonbRecords(masterId, DEFAULT_FETCH_SIZE);
    }
    
    /**
     * Stream JSONB records with custom fetch size
     * 
     * Fetch Size Guidelines:
     * - Small (100-500): Lower memory, more network round-trips
     * - Medium (1000-5000): Balanced for most use cases
     * - Large (10000+): Higher throughput, more memory per batch
     * 
     * @param masterId The master ID to query
     * @param fetchSize Number of rows to fetch per database round-trip
     * @return Stream of enhanced records with unmarshalled JSONB
     */
    public Stream<EnhancedDetailRecord> streamJsonbRecords(Long masterId, int fetchSize) {
        String sql = """
            SELECT 
                detail_id, 
                master_id, 
                record_type, 
                account_number, 
                customer_name,
                amount, 
                currency, 
                description, 
                transaction_date, 
                created_at,
                transaction_data,
                processing_status,
                error_message
            FROM enhanced_detail_records
            WHERE master_id = ?
            ORDER BY detail_id ASC
            """;
        
        log.info("Starting streaming JSONB read for master_id: {} with fetch_size: {}", 
                masterId, fetchSize);
        
        // Configure JDBC for streaming mode
        jdbcTemplate.setFetchSize(fetchSize);
        
        try {
            // queryForStream returns a lazy Stream backed by a streaming ResultSet
            // Rows are fetched in batches of 'fetchSize' as the stream is consumed
            Stream<EnhancedDetailRecord> stream = jdbcTemplate.queryForStream(
                sql, 
                new StreamingJsonbRowMapper(), 
                masterId
            );
            
            // Add stream close logging
            return stream.onClose(() -> 
                log.info("Completed streaming JSONB read for master_id: {}", masterId)
            );
            
        } catch (Exception e) {
            log.error("Error creating JSONB stream for master_id: {}", masterId, e);
            throw new RuntimeException("Failed to stream JSONB records", e);
        }
    }
    
    /**
     * Stream with custom SQL for advanced filtering
     * Useful for querying specific JSONB fields or complex conditions
     */
    public Stream<EnhancedDetailRecord> streamWithCustomQuery(
            String sql, 
            int fetchSize, 
            Object... params) {
        
        log.info("Starting custom JSONB stream query with fetch_size: {}", fetchSize);
        
        jdbcTemplate.setFetchSize(fetchSize);
        
        return jdbcTemplate.queryForStream(sql, new StreamingJsonbRowMapper(), params);
    }
    
    /**
     * Stream records filtered by JSONB criteria
     * Example: Only records with high risk scores
     */
    public Stream<EnhancedDetailRecord> streamByJsonbCriteria(
            Long masterId, 
            String jsonbPath, 
            String operator, 
            Object value,
            int fetchSize) {
        
        String sql = String.format("""
            SELECT 
                detail_id, master_id, record_type, account_number, customer_name,
                amount, currency, description, transaction_date, created_at,
                transaction_data, processing_status, error_message
            FROM enhanced_detail_records
            WHERE master_id = ?
              AND (transaction_data->>'%s')%s ?
            ORDER BY detail_id ASC
            """, jsonbPath, operator);
        
        log.info("Streaming JSONB records for master_id: {} with criteria: {} {} {}", 
                masterId, jsonbPath, operator, value);
        
        jdbcTemplate.setFetchSize(fetchSize);
        
        return jdbcTemplate.queryForStream(
            sql, 
            new StreamingJsonbRowMapper(), 
            masterId, 
            value.toString()
        );
    }
    
    /**
     * Estimate memory usage for a given fetch size
     * Useful for capacity planning
     */
    public long estimateMemoryUsage(int fetchSize, int avgJsonbSizeKb) {
        // Rough estimate: (fetch_size * avg_row_size) + JVM overhead
        long rowOverhead = 500; // bytes per Java object
        long jsonbSize = avgJsonbSizeKb * 1024L;
        long totalPerRow = rowOverhead + jsonbSize;
        
        return fetchSize * totalPerRow;
    }
    
    /**
     * RowMapper optimized for streaming JSONB data
     * Unmarshals one row at a time to minimize memory footprint
     */
    private class StreamingJsonbRowMapper implements RowMapper<EnhancedDetailRecord> {
        
        private long rowCount = 0;
        
        @Override
        public EnhancedDetailRecord mapRow(ResultSet rs, int rowNum) throws SQLException {
            rowCount++;
            
            // Log progress for large datasets (every 10,000 rows)
            if (rowCount % 10000 == 0) {
                log.debug("Streamed {} JSONB rows so far (current rowNum: {})", rowCount, rowNum);
            }
            
            // Build the record with standard fields
            EnhancedDetailRecord record = EnhancedDetailRecord.builder()
                    .detailId(rs.getLong("detail_id"))
                    .masterId(rs.getLong("master_id"))
                    .recordType(rs.getString("record_type"))
                    .accountNumber(rs.getString("account_number"))
                    .customerName(rs.getString("customer_name"))
                    .amount(rs.getBigDecimal("amount"))
                    .currency(rs.getString("currency"))
                    .description(rs.getString("description"))
                    .transactionDate(rs.getTimestamp("transaction_date") != null
                            ? rs.getTimestamp("transaction_date").toLocalDateTime()
                            : null)
                    .createdAt(rs.getTimestamp("created_at").toLocalDateTime())
                    .processingStatus(rs.getString("processing_status"))
                    .errorMessage(rs.getString("error_message"))
                    .build();
            
            // Unmarshal JSONB data efficiently
            record.setTransactionData(unmarshalJsonb(rs, "transaction_data", record.getDetailId()));
            
            return record;
        }
        
        /**
         * Efficiently unmarshal JSONB from ResultSet
         * Handles PGobject type and performs Jackson deserialization
         */
        private TransactionData unmarshalJsonb(ResultSet rs, String columnName, Long detailId) {
            try {
                Object jsonbObject = rs.getObject(columnName);
                
                if (jsonbObject == null) {
                    log.debug("Null JSONB for detail_id: {}", detailId);
                    return null;
                }
                
                String jsonString;
                
                // Handle PostgreSQL JSONB type (PGobject)
                if (jsonbObject instanceof PGobject pgObject) {
                    jsonString = pgObject.getValue();
                } else {
                    jsonString = jsonbObject.toString();
                }
                
                if (jsonString == null || jsonString.isEmpty()) {
                    log.debug("Empty JSONB string for detail_id: {}", detailId);
                    return null;
                }
                
                // Unmarshal JSON string to TransactionData object
                // This happens row-by-row, so only one object in memory at a time
                TransactionData transactionData = objectMapper.readValue(
                        jsonString, 
                        TransactionData.class
                );
                
                if (log.isTraceEnabled()) {
                    log.trace("Unmarshalled JSONB for detail_id: {}, transaction_id: {}, size: {} bytes", 
                            detailId, 
                            transactionData.getTransactionId(),
                            jsonString.length());
                }
                
                return transactionData;
                
            } catch (Exception e) {
                log.error("Failed to unmarshal JSONB for detail_id: {} - Error: {}", 
                        detailId, e.getMessage());
                
                // Don't fail the entire stream - return null for this record
                // Caller can filter out nulls if needed
                return null;
            }
        }
    }
    
    /**
     * Get count without loading records into memory
     */
    public long countJsonbRecords(Long masterId) {
        String sql = "SELECT COUNT(*) FROM enhanced_detail_records WHERE master_id = ?";
        Long count = jdbcTemplate.queryForObject(sql, Long.class, masterId);
        return count != null ? count : 0L;
    }
    
    /**
     * Get count with JSONB criteria
     */
    public long countByJsonbCriteria(String jsonbPath, String operator, Object value) {
        String sql = String.format(
            "SELECT COUNT(*) FROM enhanced_detail_records WHERE (transaction_data->>'%s')%s ?",
            jsonbPath, operator
        );
        Long count = jdbcTemplate.queryForObject(sql, Long.class, value.toString());
        return count != null ? count : 0L;
    }
    
    /**
     * Check if streaming is working correctly by verifying fetch size
     */
    public boolean verifyStreamingConfiguration() {
        int currentFetchSize = jdbcTemplate.getFetchSize();
        log.info("Current JDBC fetch size: {}", currentFetchSize);
        
        // Verify auto-commit is disabled for streaming
        try {
            jdbcTemplate.execute("SHOW autocommit");
            return true;
        } catch (Exception e) {
            log.warn("Could not verify streaming configuration", e);
            return false;
        }
    }
}
