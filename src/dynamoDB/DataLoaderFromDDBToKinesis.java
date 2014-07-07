package dynamoDB;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.CreateStreamRequest;
import com.amazonaws.services.kinesis.model.DeleteStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordResult;

public class DataLoaderFromDDBToKinesis {

	/**
	 * DynamoDB setting
	 */
	// total number of sample items
	static int scanItemCount = 300;
	// number of items each scan request should return
	static int scanItemLimit = 10;
	static int parallelScanThreads = 16;
	static AmazonDynamoDBClient DDBclient = new AmazonDynamoDBClient(
			new ProfileCredentialsProvider());
	static String tableName = "UpdateDDB";

	/**
	 * local buffer between DynamoDB and Kinesis
	 */
	private static ConcurrentLinkedQueue<Map<String, AttributeValue>> itemQueue = new ConcurrentLinkedQueue<Map<String, AttributeValue>>();


	/**
	 * Kinesis setting
	 */
    static AmazonKinesisClient kinesisClient = new AmazonKinesisClient(new ProfileCredentialsProvider("default").getCredentials());
    static String streamName = "UpdateStream";
    static Integer shardNumuber = 5;

   
    public static void main(String[] args) throws Exception {

    	if(args.length != 2)
    	{
    		System.err.println("Usage: dynamoDB/DataLoaderFromDDBToKinesis tableName streamName");
    	}
    	tableName = args[0];
    	streamName = args[1];
    	
    	System.out.println("================ Loading Data from DynamoDB to Kinesis ================");
    	
    	try{
    	createStream(streamName);
    	}
    	catch(Exception e)
    	{
    		System.out.println(streamName + " already exists");
    	}
    	
    	parallelScan(tableName);
    	populateStream(streamName);
    	
    	System.out.println();
    }
    

    
    private static void populateStream(String streamName)
    {
        System.out.println("Populating stream : " + streamName + "...");
        String sequenceNumberForOrdering = null;
        int count = 0;
        while(!itemQueue.isEmpty()) {
        	Map<String, AttributeValue> item = itemQueue.remove();
        	Map<String, Object> map = new HashMap<String, Object>();
        	Utility.itemToMap(item, map);
        	
        	// Make put request
            PutRecordRequest putRecordRequest = new PutRecordRequest();
            putRecordRequest.setStreamName(streamName);
            putRecordRequest.setData(ByteBuffer.wrap(map.toString().getBytes()));            
            putRecordRequest.setPartitionKey(String.format("partitionKey-%d", shardNumuber));
            putRecordRequest.setSequenceNumberForOrdering(sequenceNumberForOrdering);
            
            // Put data in the stream
            PutRecordResult putRecordResult = kinesisClient.putRecord(putRecordRequest);
            sequenceNumberForOrdering = putRecordResult.getSequenceNumber();
           
            count++;
            if(count % 100 == 0)
            	System.out.println(count + " items are put in " + streamName + " in total");
        }
        System.out.println(count + " items are written into " + streamName + " in total");
    }
    

    
    private static void createStream(String streamName)
    {
    	// Make a create request. 
        CreateStreamRequest createStreamRequest = new CreateStreamRequest();
        createStreamRequest.setStreamName(streamName);
        createStreamRequest.setShardCount(shardNumuber);
        
        //Create the stream
        kinesisClient.createStream(createStreamRequest);
        waitForStreamToBecomeAvailable(streamName);
    }

    private static void waitForStreamToBecomeAvailable(String myStreamName) {

        System.out.println("Waiting for " + myStreamName + " to become ACTIVE...");

        long startTime = System.currentTimeMillis();
        long endTime = startTime + (10 * 60 * 1000);
        while (System.currentTimeMillis() < endTime) {
            try {
                Thread.sleep(1000 * 20);
            } catch (InterruptedException e) {
                // Ignore interruption (doesn't impact stream creation)
            }
            try {
                DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest();
                describeStreamRequest.setStreamName(myStreamName);
                // ask for no more than 10 shards at a time -- this is an optional parameter
                describeStreamRequest.setLimit(10);
                DescribeStreamResult describeStreamResponse = kinesisClient.describeStream(describeStreamRequest);

                String streamStatus = describeStreamResponse.getStreamDescription().getStreamStatus();
                System.out.println("  - current state: " + streamStatus);
                if (streamStatus.equals("ACTIVE")) {
                    return;
                }
            } catch (AmazonServiceException ase) {
                if (ase.getErrorCode().equalsIgnoreCase("ResourceNotFoundException") == false) {
                    throw ase;
                }
                throw new RuntimeException("Stream " + myStreamName + " never went active");
            }
        }
    }
    
    private static void deleteStream()
    {
        DeleteStreamRequest deleteStreamRequest = new DeleteStreamRequest();
        deleteStreamRequest.setStreamName(streamName);

        kinesisClient.deleteStream(deleteStreamRequest);
    }
    
    
    /**
	 * Parallel scan DynamoDB table and put the data in local buffer
	 * @param tableName
	 * @param itemLimit
	 * @param numberOfThreads
	 */
	private static void parallelScan(String tableName) {
		System.out.println("Scanning " + tableName + " using "
				+ parallelScanThreads + " threads " + scanItemLimit
				+ " items at a time");
		// Creating thread pool
		ExecutorService executor = Executors
				.newFixedThreadPool(parallelScanThreads);

		// Each thread will be scanning one segment
		int totalSegments = parallelScanThreads;
		for (int segment = 0; segment < totalSegments; segment++) {
			ScanSegmentTask task = new ScanSegmentTask(tableName, scanItemLimit,
					totalSegments, segment);
			executor.execute(task);
		}
		executor.shutdown();
		
		// Wait until all threads finish
		try {
			executor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			executor.shutdownNow();
			// Preserve interrupt status
			Thread.currentThread().interrupt();
		}
		System.out.println(itemQueue.size() + " items are scanned from " + tableName + " in total");
	}

	/**
	 * Runnable task for scanning a single segment of a DynamoDB table
	 */
	private static class ScanSegmentTask implements Runnable {

		private String tableName;
		// Number of items each scan request should return
		private int itemLimit;
		// Total number of segments
		private int totalSegments;
		// Segment that will be scanned with by this task
		private int segment;

		public ScanSegmentTask(String tableName, int itemLimit,
				int totalSegments, int segment) {
			this.tableName = tableName;
			this.itemLimit = itemLimit;
			this.totalSegments = totalSegments;
			this.segment = segment;
		}

		@Override
		public void run() {
/*			System.out.println("Scanning " + tableName + " segment " + segment
					+ " out of " + totalSegments + " segments " + itemLimit
					+ " items at a time...");*/
			Map<String, AttributeValue> exclusiveStartKey = null;
			int totalScannedItemCount = 0;
			int totalScanRequestCount = 0;
			try {
				while (true) {
					ScanRequest scanRequest = new ScanRequest()
							.withTableName(tableName).withLimit(itemLimit)
							.withExclusiveStartKey(exclusiveStartKey)
							.withTotalSegments(totalSegments)
							.withSegment(segment);

					ScanResult result = DDBclient.scan(scanRequest);

					totalScanRequestCount++;
					totalScannedItemCount += result.getScannedCount();

					// adding the item list to item pool
					for (Map<String, AttributeValue> item : result.getItems()) {
						itemQueue.add(item);
					}

					exclusiveStartKey = result.getLastEvaluatedKey();
					if (exclusiveStartKey == null) {
						break;
					}
				}
			} catch (AmazonServiceException ase) {
				System.err.println(ase.getMessage());
			} finally {
/*				System.out.println("Scanned " + totalScannedItemCount
						+ " items from segment " + segment + " out of "
						+ totalSegments + " of " + tableName + " with "
						+ totalScanRequestCount + " scan requests");*/
			}
		}
	}
}
