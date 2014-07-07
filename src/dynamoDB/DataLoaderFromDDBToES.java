package dynamoDB;

import static org.elasticsearch.common.xcontent.XContentFactory.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;

/***
 * This program uses parallel scan to read data from DynamoDB and put it in
 * local buffer which is a item list. Then it populates ElasticSearch with bulk
 * API.
 * 
 */
public class DataLoaderFromDDBToES {

	/**
	 * DynamoDB setting
	 */
	// total number of sample items
	static int scanItemCount = 300;
	// number of items each scan request should return
	static int scanItemLimit = 10;
	static int parallelScanThreads = 16;
	static String tableName = "default";
	static AmazonDynamoDBClient client = new AmazonDynamoDBClient(
			new ProfileCredentialsProvider());

	/**
	 * local buffer between DynamoDB and ElasticSearch
	 */
	private static ConcurrentLinkedQueue<Map<String, AttributeValue>> itemQueue = new ConcurrentLinkedQueue<Map<String, AttributeValue>>();

	/**
	 * ElasticSearch setting
	 */
/*	static Settings settings = ImmutableSettings.settingsBuilder()
			.put("cluster.name", "es-demo").build();*/
	
	static Settings settings = ImmutableSettings.settingsBuilder()
			.put("cluster.name", "es-demo").build();
	
	// Add server IP, this cluster has two machines
/*	static Client ESclient = new TransportClient(settings).addTransportAddress(
			new InetSocketTransportAddress(
					"ec2-107-23-100-103.compute-1.amazonaws.com", 9300));*/
	
/*	static Client ESclient = new TransportClient(settings).addTransportAddress(
			new InetSocketTransportAddress(
					"ELS3-ElasticSearch-1UVDRBRKZW5ID-1985996976.us-west-2.elb.amazonaws.com", 9300));*/
	
	static String DNS = "ec2-54-88-139-195.compute-1.amazonaws.com";
	static int port = 9300;
	static Client ESclient = null;
	
	
	
	static String index = "default index";
	static String type = "default type";

	public static void main(String[] args) {
		
		System.out.println("================ Loading Data from DynamoDB to ElasticSearch ================");
		
		if(args.length != 4)
		{
			System.err.println("args length: " + args.length);
			System.err.println("Usage: dynamoDB/DataLoaderFromDDBToES tableName ES_DNS ES_port indexName");
			System.exit(0);
		}

		tableName = args[0];
		DNS = args[1];
		port = Integer.parseInt(args[2]);
		index = type = args[3];
		
		ESclient = new TransportClient(settings).addTransportAddress(
				new InetSocketTransportAddress(
						DNS, port));
		
		try {
			parallelScan(tableName, scanItemLimit, parallelScanThreads);
		} catch (AmazonServiceException ase) {
			System.err.println(ase.getMessage());
		} catch (ElasticsearchException ee) {
			System.err.println(ee.getMessage());
		} finally { // This will block until all the threads are done

			// Put data to ElasticSearch after parallel scanning
			populateES();
			ESclient.close();
		}
		
		System.out.println();
	}

	/**
	 * Parallel scan DynamoDB table and put the data in local buffer
	 * 
	 * @param tableName
	 * @param itemLimit
	 * @param numberOfThreads
	 */
	private static void parallelScan(String tableName, int itemLimit,
			int numberOfThreads) {
		System.out.println("Scanning " + tableName + " using "
				+ numberOfThreads + " threads " + itemLimit
				+ " items at a time...");
		// Creating thread pool
		ExecutorService executor = Executors
				.newFixedThreadPool(numberOfThreads);

		// Each thread will be scanning one segment
		int totalSegments = numberOfThreads;
		for (int segment = 0; segment < totalSegments; segment++) {
			ScanSegmentTask task = new ScanSegmentTask(tableName, itemLimit,
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

					ScanResult result = client.scan(scanRequest);

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
			/*	System.out.println("Scanned " + totalScannedItemCount
						+ " items from segment " + segment + " out of "
						+ totalSegments + " of " + tableName + " with "
						+ totalScanRequestCount + " scan requests");*/
			}
		}
	}

	/**
	 * Read data from local buffer and write it to ElasticSearch with bulk
	 * operation. Bulk operation collects all data together and put it to
	 * ElasticSearch as one HTTP transaction
	 * 
	 * @return
	 */
	private static int populateES() {
		System.out.println("Populating ElasticSearch...");
		// The number of items that is fed into ElasticSearch
		int count = 0;
		BulkRequestBuilder bulkRequest = ESclient.prepareBulk();
		try {
			for (Map<String, AttributeValue> item : itemQueue) {
				Map<String, Object> map = new HashMap<String, Object>();
				Utility.itemToMap(item, map);
				// take the id attribute-value pair and use it as the id in the
				// index
				String id = (String) map.get("id");
				map.remove("id");

				XContentBuilder json = jsonBuilder();
				json.map(map);
				bulkRequest.add(ESclient.prepareIndex(index, type, id)
						.setSource(json));
				count++;
			}
		} catch (IOException e) {
			System.out.println("Lucene error..");
			//e.printStackTrace();
		}

		// execute bulk operation once to put all items in ElasticSearch
		BulkResponse bulkResponse = bulkRequest.execute().actionGet();
		if (bulkResponse.hasFailures()) {
			System.out.println("Error in putting item to ElasticSearch");
		}

		System.out.println(count + " items are written into " + index + " in total");
		return count;
	}

}
