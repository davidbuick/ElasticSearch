

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;

public class LowLevelParallelScan {

    // total number of sample items 
    static int scanItemCount = 300;
    // number of items each scan request should return
    static int scanItemLimit = 10;
    // number of logical segments for parallel scan
    static int parallelScanThreads = 16;
    // table that will be used for scanning
    static String tableName = "shakespeare";
    
    static AmazonDynamoDBClient client = new AmazonDynamoDBClient(new ProfileCredentialsProvider());

    public static void main(String[] args) throws Exception {
        try {
                    
            parallelScan(tableName, scanItemLimit, parallelScanThreads);
        }  
        catch (AmazonServiceException ase) {
            System.err.println(ase.getMessage());
        }  
        
        finally
        {
        	
        }
        System.out.println("good3");
    }

    private static void parallelScan(String tableName, int itemLimit, int numberOfThreads) {
        System.out.println("Scanning " + tableName + " using " + numberOfThreads + " threads " + itemLimit + " items at a time");
        ExecutorService executor = Executors.newFixedThreadPool(numberOfThreads);

        // Each thread will be scanning one segment
        int totalSegments = numberOfThreads;
        for (int segment = 0; segment < totalSegments; segment++) {

            ScanSegmentTask task = new ScanSegmentTask(tableName, itemLimit, totalSegments, segment);
            executor.execute(task);
        }

        shutDownExecutorService(executor); 
    }

    // Runnable task for scanning a single segment of a DynamoDB table
    private static class ScanSegmentTask implements Runnable {
        
        private String tableName;
        // number of items each scan request should return
        private int itemLimit;
        // Total number of segments
        private int totalSegments;      
        // Segment that will be scanned with by this task
        private int segment;
        
        public ScanSegmentTask(String tableName, int itemLimit, int totalSegments, int segment) {
            this.tableName = tableName;
            this.itemLimit = itemLimit;
            this.totalSegments = totalSegments;
            this.segment = segment;
        }
        
        @Override
        public void run() {
            System.out.println("Scanning " + tableName + " segment " + segment + " out of " + totalSegments + " segments " + itemLimit + " items at a time...");
            Map<String, AttributeValue> exclusiveStartKey = null;
            int totalScannedItemCount = 0;
            int totalScanRequestCount = 0;
            try {
                while(true) {
                    ScanRequest scanRequest = new ScanRequest()
                        .withTableName(tableName)
                        .withLimit(itemLimit)
                        .withExclusiveStartKey(exclusiveStartKey)
                        .withTotalSegments(totalSegments)
                        .withSegment(segment);
                    
                    ScanResult result = client.scan(scanRequest);
                    
                    totalScanRequestCount++;
                    totalScannedItemCount += result.getScannedCount();
                    
                    // print items returned from scan request
                    processScanResult(segment, result);
                    
                    exclusiveStartKey = result.getLastEvaluatedKey();
                    if (exclusiveStartKey == null) {
                        break;
                    }
                }
            } catch (AmazonServiceException ase) {
                System.err.println(ase.getMessage());
            } finally {
                System.out.println("Scanned " + totalScannedItemCount + " items from segment " + segment + " out of " + totalSegments + " of " + tableName + " with " + totalScanRequestCount + " scan requests");
            }
        }
    }
    

    private static void processScanResult(int segment, ScanResult result) {
        for (Map<String, AttributeValue> item : result.getItems()) {
            printItem(segment, item);
        }
    }
    
    private static void printItem(int segment, Map<String, AttributeValue> attributeList) {
        //System.out.print("Segment " + segment + ", ");
        for (Map.Entry<String, AttributeValue> item : attributeList.entrySet()) {
            String attributeName = item.getKey();
            AttributeValue value = item.getValue();
     
            System.out.print(attributeName + " "
                    + (value.getS() == null ? "" : "S=[" + value.getS() + "]")
                    + (value.getN() == null ? "" : "N=[" + value.getN() + "]")
                    + (value.getB() == null ? "" : "B=[" + value.getB() + "]")
                    + (value.getSS() == null ? "" : "SS=[" + value.getSS() + "]")
                    + (value.getNS() == null ? "" : "NS=[" + value.getNS() + "]")
                    + (value.getBS() == null ? "" : "BS=[" + value.getBS() + "]")
                    + ", ");
        }
        // Move to next line
        System.out.println();
    }
    
    private static void shutDownExecutorService(ExecutorService executor) {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            
            // Preserve interrupt status
            Thread.currentThread().interrupt();
        }
    }
}
