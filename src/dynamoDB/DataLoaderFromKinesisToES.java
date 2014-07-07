package dynamoDB;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.GetRecordsRequest;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.GetShardIteratorRequest;
import com.amazonaws.services.kinesis.model.GetShardIteratorResult;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.model.Shard;

/**
 * This program consumes data from Kinesis and puts it in ElasticSearch
 * 
 */
public class DataLoaderFromKinesisToES {

	/**
	 * Kinesis setting
	 */
	static AmazonKinesisClient kinesisClient = new AmazonKinesisClient(
			new ProfileCredentialsProvider("default").getCredentials());
	static String streamName = "defaulStreamName";
	static Integer shardNumuber = 5;

	/**
	 * ElasticSearch setting
	 */
	// Add cluster name
/*	static Settings settings = ImmutableSettings.settingsBuilder()
			.put("cluster.name", "es-demo").build();*/
	static Settings settings = ImmutableSettings.settingsBuilder()
			.put("cluster.name", "es-demo").build();
	
	// Add server IP, this cluster has two machines
	static String DNS = "ec2-107-23-100-103.compute-1.amazonaws.com";
	static int port = 9300;
	static Client ESclient = null;
	static String index = "default";
	static String type = "default";

	static BulkRequestBuilder bulkRequest = null;

	public static void main(String[] args) {

		System.out
				.println("================ Loading Data from Kinesis to ElasticSearch ================");

		if (args.length != 4) {
			
			System.err.println("args length: " + args.length);
			System.err
					.println("Usage: dynamoDB/DataLoaderFromKinesisToES streamName ElasticSearch_DNS ElasticSearch_port indexName");
			System.exit(0);
		}
		streamName = args[0];
		DNS = args[1];
		port = Integer.valueOf(args[2]);
		index = type = args[3];
		
		ESclient = new TransportClient(settings).addTransportAddress(new InetSocketTransportAddress(DNS, port));
		bulkRequest = ESclient.prepareBulk();
		
		KinesisToES(streamName, index);

		System.out.println();
	}

	/**
	 * This method uses a infinite loop to consume data from Kinesis and put it
	 * on ElasticSearch with bulk API. If there is not data consumed in one
	 * iteration, bulk API won't be executed for this iteration
	 */
	public static void KinesisToES(String streamName, String index) {
		System.out.println("Consuming data from " + streamName
				+ " and put it to " + index + "...");
		// Get all shards from the stream
		DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest();
		describeStreamRequest.setStreamName(streamName);
		List<Shard> shards = new ArrayList<>();
		String exclusiveStartShardId = null;
		do {
			describeStreamRequest
					.setExclusiveStartShardId(exclusiveStartShardId);
			DescribeStreamResult describeStreamResult = kinesisClient
					.describeStream(describeStreamRequest);
			shards.addAll(describeStreamResult.getStreamDescription()
					.getShards());
			if (describeStreamResult.getStreamDescription().getHasMoreShards()
					&& shards.size() > 0) {
				exclusiveStartShardId = shards.get(shards.size() - 1)
						.getShardId();
			} else {
				exclusiveStartShardId = null;
			}
		} while (exclusiveStartShardId != null);

		// Get Iterators from all shards
		LinkedList<String> shardIteratorQueue = new LinkedList<String>();
		for (Shard shard : shards) {
			GetShardIteratorRequest getShardIteratorRequest = new GetShardIteratorRequest();
			getShardIteratorRequest.setStreamName(streamName);
			getShardIteratorRequest.setShardId(shard.getShardId());
			getShardIteratorRequest.setShardIteratorType("TRIM_HORIZON");

			GetShardIteratorResult getShardIteratorResult = kinesisClient
					.getShardIterator(getShardIteratorRequest);
			String shardIterator = getShardIteratorResult.getShardIterator();
			shardIteratorQueue.add(shardIterator);
		}

		// Continuously read data records from all shards in sequence with a
		// queue
		int count = 0;
		while (true) {
			boolean added = false;
			// Make get Record Request
			GetRecordsRequest getRecordsRequest = new GetRecordsRequest();
			// Remove the iterator of the current shard from the head of the
			// queue
			getRecordsRequest
					.setShardIterator(shardIteratorQueue.removeFirst());
			getRecordsRequest.setLimit(1000);
			GetRecordsResult result = kinesisClient
					.getRecords(getRecordsRequest);

			// Put result into record list. Result may be empty.
			List<Record> records = result.getRecords();
			for (Record record : records) {
				byte[] array = record.getData().array();
				String str = new String(array);
				// Discard '{' and '}' at the first and last character
				str = str.substring(1, str.length() - 1);

				Map<String, Object> map = new HashMap<String, Object>();
				Utility.stringToMap(str, map);

				// Utility.stringToMap does not guarantee the all the generated
				// maps are valid
				// If the record is invalid, just skip it
				if (map.size() != 8)
					continue;

				String id = (String) map.get("id");
				map.remove("id");

				count++;
				bulkRequest.add(ESclient.prepareIndex(index, type, id)
						.setSource(map));
				added = true;
			}
			try {
				Thread.sleep(1000);
			} catch (InterruptedException exception) {
				exception.printStackTrace();
			}

			// Execute bulk operation once to put all items in
			// ElasticSearch
			if (added) {
				BulkResponse bulkResponse = bulkRequest.execute().actionGet();
				if (bulkResponse.hasFailures()) {
					System.out
							.println("Error in putting item to ElasticSearch");
				}
				bulkRequest = ESclient.prepareBulk();

				System.out.println(count + " itmes are put from " + streamName
						+ " to " + index + " in total");
			}

			// Add the iterator of the current shard back to the end of the
			// queue
			shardIteratorQueue.add(result.getNextShardIterator());
		}
	}
}
