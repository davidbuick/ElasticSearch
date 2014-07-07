package dynamoDB;

import java.util.Map;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;

public class Utility {

	/***
	 * This method takes an item of DynamoDB and transfer it to a Map
	 * 
	 * @param item
	 * @param map
	 */
	public static void itemToMap(Map<String, AttributeValue> item,
			Map<String, Object> map) {
		// an item consists of entries. Each entry is a attribute-value pair
		// each item is a map of all the pairs
		for (Map.Entry<String, AttributeValue> entry : item.entrySet()) {
			String attributeName = entry.getKey();
			AttributeValue value = entry.getValue();

			// since the order of the attribute-value pair is arbitrary
			// we need to adjudge the type before get the value
			Object ob = null;
			if (value.getS() != null) {
				ob = value.getS();
			} else if (value.getN() != null) {
				ob = value.getN();
			} else if (value.getB() != null) {
				ob = value.getB();
			} else if (value.getSS() != null) {
				ob = value.getSS();
			} else if (value.getNS() != null) {
				ob = value.getNS();
			} else if (value.getBS() != null) {
				ob = value.getBS();
			}
			map.put(attributeName, ob);
		}
	}

	public static void mapToItem(Map<String, Object> map,
			Map<String, AttributeValue> item) {

		for (String key : map.keySet()) {
			Object value = map.get(key);
			if (value instanceof Integer) {
				item.put(key, new AttributeValue().withN(Integer
						.toString((Integer) value)));
			} else if (value instanceof String) {
				item.put(key, new AttributeValue().withS((String) value));
			}
		}
	}
	
	/*
	 * Sparse a Map formatted String to a map 
	 */
	public static void stringToMap(String str, Map<String, Object> map)
	{
		String[] entries = str.split(", ");
		for(String entry: entries)
		{
			String[] keyValue = entry.split("=");
			if(keyValue.length == 2) // successful parsing
			{
				map.put(keyValue[0], keyValue[1]);
			}
			else // Put "n/a" to indicate unsuccessful parsing
			{
				map.put(keyValue[0], "n/a");
			}
		}
	}

}
