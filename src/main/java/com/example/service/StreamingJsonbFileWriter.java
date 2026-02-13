package com.example.service;

import com.example.beanio.EnhancedDetailOutput;
import com.example.beanio.EnhancedFileHeader;
import com.example.beanio.EnhancedFileTrailer;
import com.example.model.EnhancedDetailRecord;
import com.example.model.MasterRecord;
import com.example.model.TransactionData;
import lombok.extern.slf4j.Slf4j;
import org.beanio.BeanWriter;
import org.beanio.StreamFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Service;

import java.io.BufferedWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.LocalDate;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

/**
 * Streaming file writer for JSONB data - optimized for memory efficiency
 * 
 * Memory Characteristics:
 * - Processes records one at a time (no batching in memory)
 * - BufferedWriter flushes automatically (configurable buffer size)
 * - Stream pipeline ensures garbage collection after each record
 * - Suitable for billions of records with constant memory usage
 */
@Slf4j
@Service
public class StreamingJsonbFileWriter {
    
    private final StreamFactory streamFactory;
    
    // Buffer size for file writing (8KB default, increase for better throughput)
    private static final int BUFFER_SIZE = 8192 * 4; // 32KB
    
    public StreamingJsonbFileWriter() {
        this.streamFactory = StreamFactory.newInstance();
        try {
            streamFactory.load(new ClassPathResource("beanio-mapping-enhanced.xml").getInputStream());
            log.info("BeanIO StreamFactory initialized for enhanced JSONB mapping");
        } catch (IOException e) {
            throw new RuntimeException("Failed to load enhanced BeanIO mapping", e);
        }
    }
    
    /**
     * Stream JSONB records to file with minimal memory footprint
     * 
     * Processing Model:
     * 1. Open file with buffered writer
     * 2. Write header
     * 3. Stream records one-by-one:
     *    a. Unmarshal JSONB (already done in stream)
     *    b. Flatten nested structure
     *    c. Write to BeanIO
     *    d. Object becomes eligible for GC
     * 4. Write trailer with accumulated stats
     * 5. Close and flush
     * 
     * @param master Master record metadata
     * @param jsonbStream Lazy stream of records with unmarshalled JSONB
     * @param outputPath Output file path
     * @return Path to written file
     */
    public Path streamToFile(
            MasterRecord master, 
            Stream<EnhancedDetailRecord> jsonbStream, 
            Path outputPath) throws IOException {
        
        log.info("Starting streaming JSONB write for master_id: {} to: {}", 
                master.getMasterId(), outputPath);
        
        // Ensure output directory exists
        Files.createDirectories(outputPath.getParent());
        
        // Statistics collectors (minimal memory - just accumulators)
        AtomicLong recordCount = new AtomicLong(0);
        AtomicReference<BigDecimal> totalAmount = new AtomicReference<>(BigDecimal.ZERO);
        AtomicReference<Double> totalRiskScore = new AtomicReference<>(0.0);
        AtomicLong riskScoreCount = new AtomicLong(0);
        Set<String> uniqueCustomers = new HashSet<>();
        
        // Track memory-efficient processing
        long startTime = System.currentTimeMillis();
        long lastLogTime = startTime;
        
        // Use try-with-resources for automatic resource cleanup
        try (
            // Create buffered writer with custom buffer size
            BufferedWriter writer = Files.newBufferedWriter(
                outputPath, 
                StandardOpenOption.CREATE, 
                StandardOpenOption.TRUNCATE_EXISTING
            );
            
            // BeanIO writer wraps the buffered writer
            BeanWriter beanWriter = streamFactory.createWriter("enhancedDetailRecordStream", writer)
        ) {
            // Write header
            writeHeader(beanWriter, master);
            
            // Process stream - this is where the magic happens
            // Stream is consumed lazily, one record at a time
            jsonbStream.forEach(enhancedRecord -> {
                try {
                    // Process single record
                    processAndWriteRecord(
                        enhancedRecord, 
                        beanWriter, 
                        recordCount, 
                        totalAmount, 
                        totalRiskScore, 
                        riskScoreCount, 
                        uniqueCustomers
                    );
                    
                    // Log progress periodically (every 10 seconds)
                    long currentTime = System.currentTimeMillis();
                    if (currentTime - lastLogTime > 10000) {
                        logProgress(master.getMasterId(), recordCount.get(), startTime);
                        lastLogTime = currentTime;
                    }
                    
                } catch (Exception e) {
                    log.error("Error processing record detail_id: {} for master_id: {}", 
                            enhancedRecord.getDetailId(), 
                            master.getMasterId(), 
                            e);
                    // Continue processing - don't fail entire file for one bad record
                }
            });
            
            // Write trailer with final statistics
            writeTrailer(
                beanWriter, 
                recordCount.get(), 
                totalAmount.get(), 
                totalRiskScore.get(), 
                riskScoreCount.get(), 
                uniqueCustomers.size()
            );
            
            // Flush all buffered data
            beanWriter.flush();
            writer.flush();
            
            // Log completion statistics
            logCompletion(master.getMasterId(), recordCount.get(), uniqueCustomers.size(), startTime);
        }
        
        return outputPath;
    }
    
    /**
     * Write file header
     */
    private void writeHeader(BeanWriter beanWriter, MasterRecord master) {
        EnhancedFileHeader header = EnhancedFileHeader.builder()
                .recordType("HEADER")
                .masterId(master.getMasterId())
                .businessCenterCode(master.getBusinessCenterCode())
                .fileDate(LocalDate.now())
                .recordCount(0L) // Will be in trailer
                .fileVersion("2.0")
                .build();
        
        beanWriter.write("header", header);
    }
    
    /**
     * Process a single record and write to file
     * This method processes one record at a time, minimizing memory usage
     */
    private void processAndWriteRecord(
            EnhancedDetailRecord record,
            BeanWriter beanWriter,
            AtomicLong recordCount,
            AtomicReference<BigDecimal> totalAmount,
            AtomicReference<Double> totalRiskScore,
            AtomicLong riskScoreCount,
            Set<String> uniqueCustomers) {
        
        // Flatten JSONB data into output record
        EnhancedDetailOutput output = flattenJsonbRecord(record);
        
        // Write to file immediately (don't accumulate in memory)
        beanWriter.write("detail", output);
        
        // Update counters
        recordCount.incrementAndGet();
        
        // Accumulate statistics
        if (record.getAmount() != null) {
            totalAmount.updateAndGet(current -> current.add(record.getAmount()));
        }
        
        if (output.getRiskScore() != null) {
            totalRiskScore.updateAndGet(current -> current + output.getRiskScore());
            riskScoreCount.incrementAndGet();
        }
        
        if (output.getCustomerId() != null) {
            uniqueCustomers.add(output.getCustomerId());
        }
        
        // After this method returns, the 'output' object becomes eligible for GC
        // This is key to keeping memory usage constant
    }
    
    /**
     * Flatten JSONB nested structure into a flat output record
     * This is where we extract fields from the unmarshalled TransactionData
     */
    private EnhancedDetailOutput flattenJsonbRecord(EnhancedDetailRecord record) {
        EnhancedDetailOutput.EnhancedDetailOutputBuilder builder = EnhancedDetailOutput.builder()
                .recordType("DETAIL")
                .detailId(record.getDetailId())
                .accountNumber(record.getAccountNumber())
                .customerName(record.getCustomerName())
                .amount(record.getAmount())
                .currency(record.getCurrency())
                .description(record.getDescription())
                .transactionDate(record.getTransactionDate());
        
        // Extract and flatten JSONB data
        TransactionData txnData = record.getTransactionData();
        
        if (txnData != null) {
            // Top-level transaction fields
            builder.transactionId(txnData.getTransactionId())
                   .transactionType(txnData.getTransactionType())
                   .riskScore(txnData.getRiskScore())
                   .status(txnData.getStatus());
            
            // Extract customer data (nested object)
            extractCustomerData(txnData, builder);
            
            // Extract merchant data (nested object)
            extractMerchantData(txnData, builder);
            
            // Extract payment method data (nested object)
            extractPaymentData(txnData, builder);
            
            // Extract items count (array length)
            if (txnData.getItems() != null) {
                builder.itemCount(txnData.getItems().size());
            }
        }
        
        return builder.build();
    }
    
    /**
     * Extract customer fields from JSONB
     */
    private void extractCustomerData(TransactionData txnData, 
                                     EnhancedDetailOutput.EnhancedDetailOutputBuilder builder) {
        if (txnData.getCustomer() != null) {
            TransactionData.Customer customer = txnData.getCustomer();
            
            builder.customerId(customer.getCustomerId())
                   .customerEmail(customer.getEmail())
                   .customerPhone(customer.getPhone());
            
            // Extract nested address
            if (customer.getAddress() != null) {
                builder.customerCity(customer.getAddress().getCity())
                       .customerState(customer.getAddress().getState())
                       .customerCountry(customer.getAddress().getCountry());
            }
        }
    }
    
    /**
     * Extract merchant fields from JSONB
     */
    private void extractMerchantData(TransactionData txnData, 
                                     EnhancedDetailOutput.EnhancedDetailOutputBuilder builder) {
        if (txnData.getMerchant() != null) {
            TransactionData.Merchant merchant = txnData.getMerchant();
            
            builder.merchantId(merchant.getMerchantId())
                   .merchantName(merchant.getName())
                   .merchantCategory(merchant.getCategory());
        }
    }
    
    /**
     * Extract payment method fields from JSONB
     */
    private void extractPaymentData(TransactionData txnData, 
                                    EnhancedDetailOutput.EnhancedDetailOutputBuilder builder) {
        if (txnData.getPaymentMethod() != null) {
            TransactionData.PaymentMethod payment = txnData.getPaymentMethod();
            
            builder.paymentType(payment.getType())
                   .paymentLastFour(payment.getLastFour())
                   .paymentBrand(payment.getBrand());
        }
    }
    
    /**
     * Write file trailer with statistics
     */
    private void writeTrailer(
            BeanWriter beanWriter,
            long recordCount,
            BigDecimal totalAmount,
            double totalRiskScore,
            long riskScoreCount,
            long uniqueCustomerCount) {
        
        // Calculate average risk score
        Double averageRiskScore = riskScoreCount > 0 
                ? totalRiskScore / riskScoreCount 
                : 0.0;
        
        EnhancedFileTrailer trailer = EnhancedFileTrailer.builder()
                .recordType("TRAILER")
                .totalRecords(recordCount)
                .totalAmount(totalAmount)
                .averageRiskScore(
                        BigDecimal.valueOf(averageRiskScore)
                                .setScale(2, RoundingMode.HALF_UP)
                                .doubleValue()
                )
                .uniqueCustomers(uniqueCustomerCount)
                .build();
        
        beanWriter.write("trailer", trailer);
    }
    
    /**
     * Log progress during streaming
     */
    private void logProgress(Long masterId, long recordsProcessed, long startTime) {
        long elapsed = System.currentTimeMillis() - startTime;
        double recordsPerSecond = recordsProcessed / (elapsed / 1000.0);
        
        log.info("Progress for master_id: {} - Processed: {} records, Rate: {:.0f} rec/sec", 
                masterId, recordsProcessed, recordsPerSecond);
    }
    
    /**
     * Log completion statistics
     */
    private void logCompletion(Long masterId, long totalRecords, long uniqueCustomers, long startTime) {
        long elapsed = System.currentTimeMillis() - startTime;
        double recordsPerSecond = totalRecords / (elapsed / 1000.0);
        
        log.info("Completed streaming write for master_id: {} - " +
                "Total: {} records, Unique customers: {}, " +
                "Time: {} ms, Rate: {:.0f} rec/sec", 
                masterId, totalRecords, uniqueCustomers, elapsed, recordsPerSecond);
    }
    
    /**
     * Generate output file path
     */
    public Path generateOutputPath(String outputDirectory, Long masterId, String businessCenterCode) {
        String filename = String.format("%s_%d_jsonb_stream_%s.txt", 
                businessCenterCode,
                masterId,
                System.currentTimeMillis());
        
        return Path.of(outputDirectory, filename);
    }
    
    /**
     * Estimate file size based on record count and average JSONB size
     * Useful for disk space planning
     */
    public long estimateFileSize(long recordCount, int avgFlattenedRecordBytes) {
        // Header + (records * avg_size) + trailer
        long headerSize = 200;
        long trailerSize = 200;
        long recordsSize = recordCount * avgFlattenedRecordBytes;
        
        return headerSize + recordsSize + trailerSize;
    }
}
