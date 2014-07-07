package dynamoDB;

import java.util.Map;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;

public class Tweet {

	String id;
	String createdAt;
	String text;
	String userID;
	String userName;
	String userLocation;
	String userLang;
	String userFollowers;
	
	public Tweet(String id, String createdAt, String text, String userID, String userName, String userLocation, String userLang, String userFollowers)
	{
		//Avoid empty String in Tweet, because empty String is not allowed in a DynamoDB item 
	 	if(userLocation.equals(""))
	 		userLocation = "n/a";
	 	if(userLang.equals(""))
	 		userLang = "n/a";
	 	if(text.equals(""))
	 		text = "n/a";
	 	if(userName.equals(""))
	 		userName = "n/a";
	 	
		this.id = id;
		this.createdAt = createdAt;
		this.text = text;
		this.userID = userID;
		this.userName = userName;
		this.userLocation = userLocation;
		this.userLang = userLang;
		this.userFollowers = userFollowers;
		
	}
	
	/**
	 * Add Tweet attributes to a Map
	 * @param map
	 */
	public void addToMap(Map<String, Object> map)
	{
        map.put("id", id);
        map.put("createdAt", createdAt);
        map.put("text", text);
        map.put("userID", userID);
        map.put("userName", userName);
        map.put("userLocation", userLocation);
        map.put("userLang", userLang);
        map.put("userFollowers", userFollowers);	
	}
}
