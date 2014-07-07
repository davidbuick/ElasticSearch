package dynamoDB;

/**
 * This program creates an DynamoDB table, then reads local JSON data and populates the DynamoDB table.
 * 
 */
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.util.Tables;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class DataLoaderToDDB {

	/**
	 * Local File settings
	 */
	static String fileName = "/home/local/ANT/waha/Downloads/tweets";

	/**
	 * DynamoDB settings
	 */
	static AmazonDynamoDBClient client = new AmazonDynamoDBClient(
			new ProfileCredentialsProvider("default").getCredentials());
	static String tableName = "defaultTableName";

	public static void main(String[] args) throws JsonProcessingException,
			IOException {

		if (args.length != 2) {
			System.err
					.println("Usage: dynomoDB/DataLoaderToDDB fileName tableName");
			System.exit(0);
		}

		fileName = args[0];
		tableName = args[1];
		
		System.out.println("================ Loading Data from local file to DynamoDB ================");

		client.setRegion(Region.getRegion(Regions.US_EAST_1));
		createTable(tableName);
		populateTable(tableName);
		
		System.out.println();
	}

	public static void populateTable(String tableName) {
		System.out.println("Populating table " + tableName + "...");
		BufferedReader reader = null;
		try {
			// Open a local JSON file
			reader = new BufferedReader(new FileReader(new File(fileName)));
		} catch (Exception e) {
			e.printStackTrace();
		}

		ObjectMapper mapper = new ObjectMapper();

		int count = 0;
		String str = null;
		try {
			while ((str = reader.readLine()) != null) {
				// Skip empty line
				if (str.length() == 0)
					continue;

				try {
					// Read data from a local file
					// Map a JSON to a tree
					JsonNode rootNode = mapper.readTree(str);
					// Get the sub node and interpret it as corresponding types
					JsonNode idNode = rootNode.path("id_str");
					String id = idNode.asText();

					JsonNode createdAtNode = rootNode.path("created_at");
					String createdAt = createdAtNode.asText();

					JsonNode textNode = rootNode.path("text");
					String text = textNode.asText();

					JsonNode userNode = rootNode.path("user");
					String userID = userNode.path("id_str").asText();
					String userName = userNode.path("name").asText();
					String userLocation = userNode.path("location").asText();
					String userLang = userNode.path("lang").asText();
					String userFollowers = Integer.toString(userNode.path(
							"followers_count").asInt());

					count++;

					// Create an Tweet and an item, then put the Tweet in the
					// item
					Tweet tweet = new Tweet(id, createdAt, text, userID,
							userName, userLocation, userLang, userFollowers);
					Map<String, Object> map = new HashMap<String, Object>();
					tweet.addToMap(map);
					Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
					Utility.mapToItem(map, item);

					// Put an item in DanamoDB
					PutItemRequest putItemRequest = new PutItemRequest(
							tableName, item);
					PutItemResult putItemResult = client
							.putItem(putItemRequest);

					if (count % 100 == 0) {
						System.out.println(count + " items are put in table "
								+ tableName + " in total");
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			reader.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		System.out.println(count + " items are put in table " + tableName
				+ " in total");
	}

	/**
	 * Create a table in DynamoDB
	 * 
	 * @param tableName
	 */
	public static void createTable(String tableName) {
		try {
			// Create table if it does not exist yet
			if (Tables.doesTableExist(client, tableName)) {
				System.out.println("Table " + tableName + " is already ACTIVE");
			} else {
				// Create a table with a primary hash key named 'name', which
				// holds a string
				CreateTableRequest createTableRequest = new CreateTableRequest()
						.withTableName(tableName)
						.withKeySchema(
								new KeySchemaElement().withAttributeName("id")
										.withKeyType(KeyType.HASH))
						.withAttributeDefinitions(
								new AttributeDefinition().withAttributeName(
										"id").withAttributeType(
										ScalarAttributeType.S))
						.withProvisionedThroughput(
								new ProvisionedThroughput()
										.withReadCapacityUnits(5L)
										.withWriteCapacityUnits(10L));
				TableDescription createdTableDescription = client.createTable(
						createTableRequest).getTableDescription();
				System.out.println("Created Table: " + tableName);

				// Wait for it to become active
				System.out.println("Waiting for " + tableName
						+ " to become ACTIVE...");
				Tables.waitForTableToBecomeActive(client, tableName);
			}
		} catch (AmazonServiceException ase) {
			System.out
					.println("Caught an AmazonServiceException, which means your request made it "
							+ "to AWS, but was rejected with an error response for some reason.");
			System.out.println("Error Message:    " + ase.getMessage());
			System.out.println("HTTP Status Code: " + ase.getStatusCode());
			System.out.println("AWS Error Code:   " + ase.getErrorCode());
			System.out.println("Error Type:       " + ase.getErrorType());
			System.out.println("Request ID:       " + ase.getRequestId());
		} catch (AmazonClientException ace) {
			System.out
					.println("Caught an AmazonClientException, which means the client encountered "
							+ "a serious internal problem while trying to communicate with AWS, "
							+ "such as not being able to access the network.");
			System.out.println("Error Message: " + ace.getMessage());
		}
	}
}
