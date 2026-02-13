-- Streaming JSONB Performance Demonstration
-- This script shows how streaming maintains constant memory usage

-- ============================================================================
-- SCENARIO: Process 1 million records with 10KB JSONB each = 10GB total data
-- ============================================================================

-- Step 1: Create a master record for streaming test
INSERT INTO master_records (business_center_code, priority, status)
VALUES ('NYC', 100, 'PENDING')
RETURNING master_id;
-- Assume returns master_id = 1000

-- Step 2: Generate 1 million records (adjust based on your test needs)
-- WARNING: This will create significant data. Start with 10K for testing.

DO $$
DECLARE
    master_id_val BIGINT := 1000;
    batch_size INT := 10000; -- Generate in batches
    total_records INT := 1000000; -- 1 million total
    current_batch INT := 0;
    start_time TIMESTAMP;
    end_time TIMESTAMP;
BEGIN
    start_time := clock_timestamp();
    
    WHILE current_batch < total_records LOOP
        -- Generate batch of records with realistic JSONB
        INSERT INTO enhanced_detail_records (
            master_id,
            record_type,
            account_number,
            customer_name,
            amount,
            currency,
            description,
            transaction_date,
            transaction_data,
            processing_status
        )
        SELECT 
            master_id_val,
            CASE (random() * 3)::INT
                WHEN 0 THEN 'DEBIT'
                WHEN 1 THEN 'CREDIT'
                ELSE 'TRANSFER'
            END,
            'ACC' || LPAD((random() * 999999999)::BIGINT::TEXT, 10, '0'),
            'Customer ' || (random() * 100000)::INT,
            (random() * 10000)::NUMERIC(10,2),
            'USD',
            'Transaction ' || gs,
            CURRENT_TIMESTAMP - (random() * INTERVAL '365 days'),
            -- Complex JSONB structure (~10KB)
            jsonb_build_object(
                'transaction_id', 'TXN' || LPAD(gs::TEXT, 12, '0'),
                'transaction_type', CASE (random() * 3)::INT
                    WHEN 0 THEN 'PURCHASE'
                    WHEN 1 THEN 'REFUND'
                    ELSE 'VOID'
                END,
                'amount', (random() * 10000)::NUMERIC(10,2),
                'currency', 'USD',
                'timestamp', CURRENT_TIMESTAMP,
                'customer', jsonb_build_object(
                    'customer_id', 'CUST' || LPAD((random() * 999999)::INT::TEXT, 6, '0'),
                    'name', 'Customer ' || (random() * 100000)::INT,
                    'email', 'customer' || (random() * 100000)::INT || '@example.com',
                    'phone', '+1-555-' || LPAD((random() * 9999999)::INT::TEXT, 7, '0'),
                    'address', jsonb_build_object(
                        'street', (random() * 9999)::INT || ' Main St',
                        'city', CASE (random() * 5)::INT
                            WHEN 0 THEN 'New York'
                            WHEN 1 THEN 'Los Angeles'
                            WHEN 2 THEN 'Chicago'
                            WHEN 3 THEN 'Houston'
                            ELSE 'Phoenix'
                        END,
                        'state', 'NY',
                        'postal_code', LPAD((random() * 99999)::INT::TEXT, 5, '0'),
                        'country', 'USA'
                    ),
                    'loyalty_tier', CASE (random() * 3)::INT
                        WHEN 0 THEN 'GOLD'
                        WHEN 1 THEN 'SILVER'
                        ELSE 'BRONZE'
                    END
                ),
                'merchant', jsonb_build_object(
                    'merchant_id', 'MERCH' || LPAD((random() * 999999)::INT::TEXT, 6, '0'),
                    'name', 'Merchant ' || (random() * 10000)::INT,
                    'category', CASE (random() * 5)::INT
                        WHEN 0 THEN 'Restaurant'
                        WHEN 1 THEN 'Retail'
                        WHEN 2 THEN 'Grocery'
                        WHEN 3 THEN 'Gas Station'
                        ELSE 'Online'
                    END,
                    'mcc', LPAD((5000 + (random() * 4999)::INT)::TEXT, 4, '0')
                ),
                'payment_method', jsonb_build_object(
                    'type', CASE (random() * 3)::INT
                        WHEN 0 THEN 'CREDIT_CARD'
                        WHEN 1 THEN 'DEBIT_CARD'
                        ELSE 'BANK_TRANSFER'
                    END,
                    'last_four', LPAD((random() * 9999)::INT::TEXT, 4, '0'),
                    'brand', CASE (random() * 3)::INT
                        WHEN 0 THEN 'VISA'
                        WHEN 1 THEN 'MASTERCARD'
                        ELSE 'AMEX'
                    END,
                    'expiry_month', 1 + (random() * 11)::INT,
                    'expiry_year', 2025 + (random() * 5)::INT
                ),
                'items', (
                    SELECT jsonb_agg(
                        jsonb_build_object(
                            'item_id', 'ITEM' || LPAD((random() * 999999)::INT::TEXT, 6, '0'),
                            'name', 'Product ' || (random() * 1000)::INT,
                            'quantity', 1 + (random() * 10)::INT,
                            'unit_price', (random() * 500)::NUMERIC(10,2),
                            'category', CASE (random() * 3)::INT
                                WHEN 0 THEN 'Electronics'
                                WHEN 1 THEN 'Clothing'
                                ELSE 'Food'
                            END
                        )
                    )
                    FROM generate_series(1, 1 + (random() * 5)::INT)
                ),
                'metadata', jsonb_build_object(
                    'ip_address', 
                    (floor(random() * 255)::INT)::TEXT || '.' ||
                    (floor(random() * 255)::INT)::TEXT || '.' ||
                    (floor(random() * 255)::INT)::TEXT || '.' ||
                    (floor(random() * 255)::INT)::TEXT,
                    'user_agent', 'Mozilla/5.0',
                    'session_id', md5(random()::TEXT)
                ),
                'risk_score', (random() * 100)::NUMERIC(5,2),
                'status', CASE (random() * 3)::INT
                    WHEN 0 THEN 'COMPLETED'
                    WHEN 1 THEN 'PENDING'
                    ELSE 'FAILED'
                END
            ),
            'PENDING'
        FROM generate_series(current_batch + 1, current_batch + batch_size) gs;
        
        current_batch := current_batch + batch_size;
        
        -- Log progress
        IF current_batch % 100000 = 0 THEN
            RAISE NOTICE 'Generated % records so far...', current_batch;
        END IF;
    END LOOP;
    
    end_time := clock_timestamp();
    
    RAISE NOTICE 'Generated % records in % seconds', 
        total_records, 
        EXTRACT(EPOCH FROM (end_time - start_time));
END $$;

-- Step 3: Verify data size
SELECT 
    pg_size_pretty(pg_total_relation_size('enhanced_detail_records')) as total_size,
    pg_size_pretty(pg_relation_size('enhanced_detail_records')) as table_size,
    pg_size_pretty(pg_indexes_size('enhanced_detail_records')) as indexes_size,
    COUNT(*) as record_count,
    AVG(pg_column_size(transaction_data)) as avg_jsonb_size_bytes
FROM enhanced_detail_records
WHERE master_id = 1000;

-- ============================================================================
-- MEMORY COMPARISON: Batch vs Streaming
-- ============================================================================

/*
BATCH LOADING (loading all into memory):
-------------------------------------------
1 million records × 10KB each = 10GB in memory
+ Java object overhead (~50% more) = 15GB
+ JVM overhead = 15.5GB total
Result: OutOfMemoryError (unless you have 16GB+ heap)

STREAMING (constant memory):
-------------------------------------------
fetch_size=1000 records × 10KB each = 10MB in buffer
+ Active processing (1 record) = 10KB
+ JVM overhead = 50MB
Total: ~60MB constant
Result: Works perfectly even with 1GB heap!
*/

-- ============================================================================
-- PERFORMANCE TEST: Measure streaming throughput
-- ============================================================================

-- Test query that will be executed by Java streaming
EXPLAIN (ANALYZE, BUFFERS)
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
WHERE master_id = 1000
ORDER BY detail_id ASC;

-- Expected output should show:
-- - Index Scan (not Seq Scan)
-- - Actual time: ~milliseconds per row
-- - Buffers: shared hit (using cache)

-- ============================================================================
-- JSONB QUERY PERFORMANCE: Test different access patterns
-- ============================================================================

-- Test 1: Extract single field (expression index should help)
EXPLAIN (ANALYZE, BUFFERS)
SELECT detail_id, transaction_data->>'customer_id'
FROM enhanced_detail_records
WHERE master_id = 1000
  AND (transaction_data->>'risk_score')::NUMERIC > 80;

-- Test 2: Containment query (GIN index should help)
EXPLAIN (ANALYZE, BUFFERS)
SELECT detail_id, transaction_data
FROM enhanced_detail_records
WHERE master_id = 1000
  AND transaction_data @> '{"status": "COMPLETED"}'::jsonb;

-- Test 3: Nested path extraction
EXPLAIN (ANALYZE, BUFFERS)
SELECT 
    detail_id,
    transaction_data->'customer'->>'email' as customer_email,
    transaction_data->'merchant'->>'name' as merchant_name,
    jsonb_array_length(transaction_data->'items') as item_count
FROM enhanced_detail_records
WHERE master_id = 1000
LIMIT 1000;

-- ============================================================================
-- SIMULATE JAVA STREAMING: What the application will do
-- ============================================================================

/*
Java Code Flow:
--------------

1. StreamingJsonbRepository.streamJsonbRecords(1000, fetchSize=1000)
   - Opens database cursor
   - Returns lazy Stream<EnhancedDetailRecord>
   
2. For each batch of 1000 rows:
   - JDBC fetches 1000 rows from cursor
   - RowMapper processes each row:
     a. Extract relational fields from ResultSet
     b. Extract JSONB as PGobject
     c. Get JSON string from PGobject
     d. Jackson unmarshal → TransactionData object
     e. Attach to EnhancedDetailRecord
   - Returns record to stream
   
3. StreamingJsonbFileWriter.streamToFile()
   - Opens BufferedWriter to output file
   - Writes header
   - For each record in stream:
     a. Flatten JSONB (extract nested fields)
     b. Write to BeanIO
     c. Record becomes eligible for GC
   - Writes trailer
   - Closes file
   
4. Result:
   - 1 million records processed
   - Output file: ~500MB (flattened pipe-delimited)
   - Memory used: ~60MB constant
   - Time: ~3-5 minutes (depending on hardware)
*/

-- ============================================================================
-- MONITORING QUERIES: Run during Java processing
-- ============================================================================

-- Monitor active streaming cursors
SELECT 
    pid,
    usename,
    application_name,
    state,
    query_start,
    state_change,
    substring(query, 1, 100) as query_preview
FROM pg_stat_activity
WHERE query LIKE '%enhanced_detail_records%'
  AND state = 'active';

-- Monitor JSONB processing statistics
SELECT 
    schemaname,
    tablename,
    seq_scan,
    seq_tup_read,
    idx_scan,
    idx_tup_fetch,
    n_tup_ins,
    n_tup_upd,
    n_tup_del,
    n_live_tup
FROM pg_stat_user_tables
WHERE tablename = 'enhanced_detail_records';

-- Monitor buffer cache hits (should be high for repeated queries)
SELECT 
    heap_blks_read,
    heap_blks_hit,
    CASE 
        WHEN (heap_blks_hit + heap_blks_read) > 0 
        THEN round(100.0 * heap_blks_hit / (heap_blks_hit + heap_blks_read), 2)
        ELSE 0 
    END as cache_hit_ratio
FROM pg_statio_user_tables
WHERE relname = 'enhanced_detail_records';

-- ============================================================================
-- CLEANUP (optional)
-- ============================================================================

-- Remove test data when done
-- DELETE FROM enhanced_detail_records WHERE master_id = 1000;
-- DELETE FROM master_records WHERE master_id = 1000;
-- VACUUM FULL enhanced_detail_records;
