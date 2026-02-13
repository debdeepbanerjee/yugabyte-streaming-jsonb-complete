package com.example.service;

import com.example.config.ProcessorConfigProperties;
import com.example.model.EnhancedDetailRecord;
import com.example.model.MasterRecord;
import com.example.repository.MasterRecordRepository;
import com.example.repository.StreamingJsonbRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.nio.file.Path;
import java.util.stream.Stream;

/**
 * Streaming JSONB processing service - orchestrates memory-efficient JSONB processing
 * 
 * Processing Flow:
 * 1. Lock master record
 * 2. Create streaming cursor to database (fetch size configured)
 * 3. For each row (streamed one at a time):
 *    a. JDBC fetches row from cursor
 *    b. Unmarshal JSONB to Java object
 *    c. Flatten nested structure
 *    d. Write to file via BeanIO
 *    e. Object becomes eligible for GC
 * 4. Close stream and update master status
 * 
 * Memory Profile:
 * - Constant memory usage regardless of total rows
 * - Peak memory = max(fetch_size) * avg_row_size + JVM overhead
 * - Suitable for billions of rows
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class StreamingJsonbProcessingService {
    
    private final MasterRecordRepository masterRecordRepository;
    private final StreamingJsonbRepository streamingJsonbRepository;
    private final StreamingJsonbFileWriter streamingFileWriter;
    private final ProcessorConfigProperties config;
    
    // Unique instance identifier
    private final String instanceId = generateInstanceId();
    
    /**
     * Process next available master record using streaming JSONB approach
     * 
     * @return true if a record was processed, false if no work available
     */
    public boolean processNextStreamingMaster() {
        try {
            // Find and lock next available master (same as regular processing)
            var masterIdOpt = masterRecordRepository.findNextAvailableMasterId(
                    instanceId, 
                    config.getLockTimeoutSeconds()
            );
            
            if (masterIdOpt.isEmpty()) {
                log.debug("No available master records for streaming JSONB processing");
                return false;
            }
            
            Long masterId = masterIdOpt.get();
            log.info("Processing master_id: {} with streaming JSONB on instance: {}", 
                    masterId, instanceId);
            
            // Get master record details
            MasterRecord master = masterRecordRepository.findById(masterId)
                    .orElseThrow(() -> new RuntimeException("Master record not found: " + masterId));
            
            // Process with streaming approach
            processStreamingMaster(master);
            
            return true;
            
        } catch (Exception e) {
            log.error("Error in streaming JSONB processing", e);
            return false;
        }
    }
    
    /**
     * Process a specific master record using streaming
     */
    private void processStreamingMaster(MasterRecord master) {
        Long masterId = master.getMasterId();
        
        try {
            // Get total count for logging (lightweight query)
            long totalRecords = streamingJsonbRepository.countJsonbRecords(masterId);
            
            log.info("Starting streaming JSONB processing for master_id: {} with {} total records", 
                    masterId, totalRecords);
            
            // Estimate memory usage
            int fetchSize = config.getBatchSize();
            long estimatedMemory = streamingJsonbRepository.estimateMemoryUsage(fetchSize, 10); // 10KB avg JSONB
            log.info("Estimated peak memory usage: {} MB with fetch_size: {}", 
                    estimatedMemory / (1024 * 1024), fetchSize);
            
            // Create streaming cursor - THIS IS THE KEY TO MEMORY EFFICIENCY
            // The stream is lazy - rows are only fetched as they're consumed
            Stream<EnhancedDetailRecord> jsonbStream = streamingJsonbRepository.streamJsonbRecords(
                    masterId, 
                    fetchSize
            );
            
            // Generate output path
            Path outputPath = streamingFileWriter.generateOutputPath(
                    config.getOutputDirectory(),
                    masterId,
                    master.getBusinessCenterCode()
            );
            
            // Stream to file - processes one record at a time
            // Memory usage stays constant even for billions of rows
            Path writtenFile = streamingFileWriter.streamToFile(
                    master, 
                    jsonbStream, 
                    outputPath
            );
            
            // Mark as completed
            masterRecordRepository.completeMaster(masterId, instanceId);
            
            log.info("Successfully completed streaming JSONB processing for master_id: {}, file: {}", 
                    masterId, writtenFile);
            
        } catch (Exception e) {
            log.error("Failed streaming JSONB processing for master_id: {}", masterId, e);
            masterRecordRepository.failMaster(masterId, instanceId, 
                    "Streaming error: " + e.getMessage());
            throw new RuntimeException("Failed to process streaming master: " + masterId, e);
        }
    }
    
    /**
     * Process master with custom JSONB filtering (e.g., only high-risk transactions)
     * Demonstrates advanced streaming with JSONB criteria
     */
    public void processStreamingMasterWithFilter(
            Long masterId, 
            String jsonbPath, 
            String operator, 
            Object value) {
        
        try {
            MasterRecord master = masterRecordRepository.findById(masterId)
                    .orElseThrow(() -> new RuntimeException("Master not found: " + masterId));
            
            log.info("Processing master_id: {} with JSONB filter: {} {} {}", 
                    masterId, jsonbPath, operator, value);
            
            // Stream only records matching JSONB criteria
            Stream<EnhancedDetailRecord> filteredStream = 
                    streamingJsonbRepository.streamByJsonbCriteria(
                            masterId, 
                            jsonbPath, 
                            operator, 
                            value,
                            config.getBatchSize()
                    );
            
            // Generate output path with filter indicator
            Path outputPath = Path.of(
                    config.getOutputDirectory(),
                    String.format("%s_%d_filtered_%s.txt", 
                            master.getBusinessCenterCode(),
                            masterId,
                            System.currentTimeMillis())
            );
            
            // Stream filtered records to file
            streamingFileWriter.streamToFile(master, filteredStream, outputPath);
            
            log.info("Completed filtered streaming for master_id: {}", masterId);
            
        } catch (Exception e) {
            log.error("Failed filtered streaming for master_id: {}", masterId, e);
            throw new RuntimeException("Filtered streaming failed", e);
        }
    }
    
    /**
     * Demonstrate streaming with multiple filters
     * Example: High-risk transactions from specific merchant category
     */
    public void processHighRiskMerchantTransactions(Long masterId, String merchantCategory) {
        try {
            MasterRecord master = masterRecordRepository.findById(masterId)
                    .orElseThrow(() -> new RuntimeException("Master not found: " + masterId));
            
            // Custom SQL with multiple JSONB conditions
            String sql = """
                SELECT 
                    detail_id, master_id, record_type, account_number, customer_name,
                    amount, currency, description, transaction_date, created_at,
                    transaction_data, processing_status, error_message
                FROM enhanced_detail_records
                WHERE master_id = ?
                  AND (transaction_data->>'risk_score')::NUMERIC > 80
                  AND transaction_data->'merchant'->>'category' = ?
                ORDER BY (transaction_data->>'risk_score')::NUMERIC DESC
                """;
            
            log.info("Processing high-risk {} transactions for master_id: {}", 
                    merchantCategory, masterId);
            
            // Stream with custom query
            Stream<EnhancedDetailRecord> highRiskStream = 
                    streamingJsonbRepository.streamWithCustomQuery(
                            sql, 
                            config.getBatchSize(), 
                            masterId, 
                            merchantCategory
                    );
            
            Path outputPath = Path.of(
                    config.getOutputDirectory(),
                    String.format("%s_%d_high_risk_%s.txt", 
                            master.getBusinessCenterCode(),
                            masterId,
                            merchantCategory.toLowerCase())
            );
            
            streamingFileWriter.streamToFile(master, highRiskStream, outputPath);
            
            log.info("Completed high-risk streaming for master_id: {}", masterId);
            
        } catch (Exception e) {
            log.error("Failed high-risk streaming for master_id: {}", masterId, e);
            throw new RuntimeException("High-risk streaming failed", e);
        }
    }
    
    /**
     * Process with parallel streams for extreme throughput
     * WARNING: Use only when I/O is the bottleneck, not CPU or memory
     */
    public void processWithParallelStreaming(Long masterId, int parallelism) {
        try {
            MasterRecord master = masterRecordRepository.findById(masterId)
                    .orElseThrow(() -> new RuntimeException("Master not found: " + masterId));
            
            log.info("Processing master_id: {} with parallel streaming (parallelism: {})", 
                    masterId, parallelism);
            
            // Create parallel stream
            Stream<EnhancedDetailRecord> parallelStream = 
                    streamingJsonbRepository.streamJsonbRecords(masterId, config.getBatchSize())
                            .parallel();
            
            Path outputPath = streamingFileWriter.generateOutputPath(
                    config.getOutputDirectory(),
                    masterId,
                    master.getBusinessCenterCode()
            );
            
            // Note: Parallel writing requires thread-safe file operations
            // For simplicity, we collect to sequential stream before writing
            streamingFileWriter.streamToFile(
                    master, 
                    parallelStream.sequential(), 
                    outputPath
            );
            
            masterRecordRepository.completeMaster(masterId, instanceId);
            
            log.info("Completed parallel streaming for master_id: {}", masterId);
            
        } catch (Exception e) {
            log.error("Failed parallel streaming for master_id: {}", masterId, e);
            throw new RuntimeException("Parallel streaming failed", e);
        }
    }
    
    /**
     * Verify streaming configuration is optimal
     */
    public void verifyStreamingSetup() {
        log.info("Verifying streaming JSONB setup for instance: {}", instanceId);
        
        boolean isConfigured = streamingJsonbRepository.verifyStreamingConfiguration();
        
        if (isConfigured) {
            log.info("✓ Streaming configuration is optimal");
            log.info("  - Fetch size: {}", config.getBatchSize());
            log.info("  - Max concurrent masters: {}", config.getMaxConcurrentMasters());
            log.info("  - Output directory: {}", config.getOutputDirectory());
        } else {
            log.warn("⚠ Streaming configuration may not be optimal");
        }
    }
    
    /**
     * Generate unique instance identifier
     */
    private String generateInstanceId() {
        String hostname = getHostname();
        long processId = ProcessHandle.current().pid();
        long threadId = Thread.currentThread().threadId();
        
        return String.format("%s-streaming-jsonb-%d-%d-%d", 
                hostname, 
                processId, 
                threadId,
                System.currentTimeMillis());
    }
    
    private String getHostname() {
        try {
            return java.net.InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            return "unknown";
        }
    }
    
    public String getInstanceId() {
        return instanceId;
    }
}
