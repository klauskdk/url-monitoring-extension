package com.appdynamics.extensions.datastore.analytics;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;

import org.apache.log4j.Logger;

public class AnalyticsDatastore {
	
	private Logger log;
	private String analyticsApiUrl;  
	private String globalAccountName;  
	private String apiKey; 

    private String schema;
    private boolean isSchemaValidated;
    
    public AnalyticsDatastore(Logger log, String analyticsApiUrl, String globalAccountName, String apiKey, String schemaName, Map<String, Object> inputMapStructure){
    	
    	this.log = log;
    	this.analyticsApiUrl = analyticsApiUrl;
    	this.globalAccountName = globalAccountName;
    	this.apiKey = apiKey;
    	this.schema = schemaName;
    	
    	if(DoSchemaExist(this.schema)){
    		this.isSchemaValidated = true;
    	}
    	else {
    		this.isSchemaValidated = CreateSchema(this.schema, inputMapStructure);
    	}
    }
    
	public Boolean PublishEvent(String schemaName, Map<String, Object> inputMap) {
    	
		Boolean successfullyCreated = false;
    	
    	try {
    		if(!DoSchemaExist(schemaName)){
    			throw new RuntimeException("Schema doesn't exist : " + schemaName);
    		}
    		
    		String sUrl = String.format(this.analyticsApiUrl + "/events/publish/%s", schemaName);
    		URL url = new URL(sUrl);
    		HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    		conn.setDoOutput(true);
    		conn.setRequestMethod("POST");
    		
    		conn.setRequestProperty("X-Events-API-AccountName", this.globalAccountName);
    		conn.setRequestProperty("X-Events-API-Key", this.apiKey);
    		conn.setRequestProperty("Content-Type", "application/vnd.appd.events+json;v=2");
    		conn.setRequestProperty("Accept", "application/vnd.appd.events+json;v=2");
    		conn.setRequestProperty("cache-control", "no-cache");
    		    		    		
    		String input = MapToInputMessage(inputMap);

    		log.info("PublishEvent: Connecting to server");
    		log.info(String.format("PublishEvent - Using schema: %s", schemaName));
    		OutputStream os = conn.getOutputStream();
    		os.write(input.getBytes());
    		os.flush();

    		if (conn.getResponseCode() != 200) {
    			throw new RuntimeException("Failed : HTTP error code : "
    				+ conn.getResponseCode() + conn.getResponseMessage());
    		}

    		BufferedReader br = new BufferedReader(new InputStreamReader(
    			(conn.getInputStream())));

    		String output;
    		log.info("PublishEvent: Output from Server ....");
    		
    		while ((output = br.readLine()) != null) {
    			log.info(output);
    		}
    		
    		log.info(String.format("Response Message: %s", conn.getResponseMessage()));

    		conn.disconnect();
    		
    		successfullyCreated = true;

    	  } catch (MalformedURLException e) {

    		  log.error("Error in PublishEvent: " + e.getMessage(), e);
    		  e.printStackTrace();

    	  } catch (IOException e) {
    		  
    		  log.error("Error in PublishEvent: " + e.getMessage(), e);
    		  e.printStackTrace();
    		  
    	  } catch(RuntimeException e){
    		  log.error("Error in PublishEvent: " + e.getMessage(), e);
    	  }
    	
    	return successfullyCreated;
    }
    
	public Boolean CreateSchema(String schemaName, Map<String, Object> schemaStructureMap){

		Boolean successfullyCreated = false;
	  
    	try {
    		if(DoSchemaExist(schemaName)){
    			throw new RuntimeException("Schema already exists : " + schemaName);
    		}
    		
    		String sUrl = String.format(this.analyticsApiUrl +"/events/schema/%s", schemaName);
    		URL url = new URL(sUrl);
    		HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    		conn.setDoOutput(true);
    		conn.setRequestMethod("POST");
    		
    		conn.setRequestProperty("X-Events-API-AccountName", this.globalAccountName);
    		conn.setRequestProperty("X-Events-API-Key", this.apiKey);
    		conn.setRequestProperty("Content-Type", "application/vnd.appd.events+json;v=2");
    		conn.setRequestProperty("Accept", "application/vnd.appd.events+json;v=2");
    		conn.setRequestProperty("cache-control", "no-cache");
    		
    		String input = MapSchemaMessage(schemaStructureMap);
    		
    		log.info("CreateSchema: Connecting to server");
    		log.info(String.format("CreateSchema - Using schema: %s", schemaName));
    		
    		OutputStream os = conn.getOutputStream();
    		os.write(input.getBytes());
    		os.flush();

    		if (conn.getResponseCode() != HttpURLConnection.HTTP_CREATED) {
    			throw new RuntimeException("Failed : HTTP error code : "
    				+ conn.getResponseCode());
    		}

    		BufferedReader br = new BufferedReader(new InputStreamReader(
    			(conn.getInputStream())));

    		String output;
    		log.info("CreateSchema: Output from Server ....");
    		
    		while ((output = br.readLine()) != null) {
    			log.info(output);
    		}
    		
    		log.info(String.format("Response Message: %s", conn.getResponseMessage()));

    		conn.disconnect();
    		
    		successfullyCreated = true;

    	  } catch (MalformedURLException e) {
    		  
    		  log.error("Error in CreateSchema: " + e.getMessage(), e);
    		  e.printStackTrace();

    	  } catch (IOException e) {

    		  log.error("Error in CreateSchema: " + e.getMessage(), e);
    		  e.printStackTrace();

    	  } catch(RuntimeException e){
    		  log.error("Error in DoSchemaExist: " + e.getMessage(), e);
    	  }
    	
    	return successfullyCreated;
    }

	private Boolean DoSchemaExist(String schemaName){
    	if(this.isSchemaValidated)
    		return true;
		
    	Boolean exists = false;
    	
    	try {
    		String sUrl = String.format(this.analyticsApiUrl + "/events/schema/%s", schemaName);
    		URL url = new URL(sUrl);
    		HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    		conn.setRequestMethod("GET");
    		
    		conn.setRequestProperty("X-Events-API-AccountName", this.globalAccountName);
    		conn.setRequestProperty("X-Events-API-Key", this.apiKey);
    		conn.setRequestProperty("Accept", "application/vnd.appd.events+json;v=2");
    		conn.setRequestProperty("cache-control", "no-cache");
    		
    		log.info("DoSchemaExist: Connecting to server");
    		log.info(String.format("DoSchemaExist - Checking schema: %s", schemaName));
    		if(conn.getResponseCode()== 200){
    			exists = true;
    		} else if(conn.getResponseCode()!= 404){
    			throw new RuntimeException("Failed : HTTP error code : "
    					+ conn.getResponseCode());
    		}

    		conn.disconnect();
    	  } catch (MalformedURLException e) {

    		  log.error("Error in DoSchemaExist: " + e.getMessage(), e);

    	  } catch (IOException e) {

    		  log.error("Error in DoSchemaExist: " + e.getMessage(), e);

    	  } catch(RuntimeException e){
    		  log.error("Error in DoSchemaExist: " + e.getMessage(), e);
    	  }
    	
    	
    	return exists;
    } 
	    
	public Boolean DeleteSchema(String schemaName){
    	Boolean isDeleted = false;
    	
    	if(DoSchemaExist(schemaName)){
	    	try {
	    		
	    		String sUrl = String.format(this.analyticsApiUrl + "/events/schema/%s", schemaName);
	    		URL url = new URL(sUrl);
	    		HttpURLConnection conn = (HttpURLConnection) url.openConnection();
	    		conn.setRequestMethod("DELETE");
	    		
	    		conn.setRequestProperty("X-Events-API-AccountName", this.globalAccountName);
	    		conn.setRequestProperty("X-Events-API-Key", this.apiKey);
	    		conn.setRequestProperty("Accept", "application/vnd.appd.events+json;v=2");
	    		conn.setRequestProperty("cache-control", "no-cache");
	    		
	    		log.info("DeleteSchema: Connecting to server");
	    		log.info(String.format("DeleteSchema - Using schema: %s", schemaName));
	    		
	    		if (conn.getResponseCode() != 200) {
	    			throw new RuntimeException("Failed : HTTP error code : "
	    					+ conn.getResponseCode());
	    		}
	
	    		BufferedReader br = new BufferedReader(new InputStreamReader(
	    			(conn.getInputStream())));
	
	    		String output;
	    		
	    		log.info("DeleteSchema: Output from Server ....");
	    		
	    		while ((output = br.readLine()) != null) {
	    			log.info(output);
	    		}
	
	    		conn.disconnect();
	    		isDeleted = true;
	
	    	  } catch (MalformedURLException e) {
	
	    		  log.error("Error in DeleteSchema: " + e.getMessage(), e);
	
	    	  } catch (IOException e) {
	
	    		  log.error("Error in DeleteSchema: " + e.getMessage(), e);
	
	    	  } catch(RuntimeException e){
	    		  
	    		  log.error("Error in DeleteSchema: " + e.getMessage(), e);
	    	  }
    	}
    	
    	return isDeleted;
    }
	
	protected String MapToInputMessage(Map<String, Object> inputMap){
		
		//format : "[{\"account\": "+account+",\"product\": \""+product+"\"}]";
		
    	StringBuilder inputBuilder = new StringBuilder();
		inputBuilder.append("[{");
		Boolean isFirstItem = true;
		
		for(String key : inputMap.keySet()){
			Object value = inputMap.get(key);
			
		
			if(!isFirstItem){
				inputBuilder.append(",");
			}
			
			if(value instanceof Integer) {
				inputBuilder.append("\"" + key + "\": " + value.toString());	
			}
			else if(value instanceof Long) {
				inputBuilder.append("\"" + key + "\": " + value.toString());
			}
			else if(value instanceof String) {
				inputBuilder.append("\"" + key + "\": \"" + value + "\"");
			}
			
			// no longer the first item when we reach this point
			isFirstItem = false;
			
		}
		inputBuilder.append("}]");
		
		String output = inputBuilder.toString();
		
		log.info(String.format("Map to Input Message - output: %s", output));
		
		return output;
    }
    
    protected String MapSchemaMessage(Map<String, Object> inputMap){
    	
    	//Format example :  "{\"qty\":100,\"name\":\"iPad 4\"}";
		//Format spec :  "{\"schema\" : {\"account\": \"integer\",\"product\": \"string\"}}";
    	
    	StringBuilder inputBuilder = new StringBuilder();
		inputBuilder.append("{\"schema\" : {");
		Boolean isFirstItem = true;
		
		for(String key : inputMap.keySet()){
			if(!isFirstItem){
				inputBuilder.append(",");
			}
			Object value = inputMap.get(key);
			if(value instanceof Integer){
				inputBuilder.append("\"" + key + "\":  \"" + "integer" + "\"");
			}
			else if(value instanceof Long){
				inputBuilder.append("\"" + key + "\":  \"" + "integer" + "\"");
			}
			else if(value instanceof String){
				inputBuilder.append("\"" + key + "\":  \"" + "string" + "\"");
			}
			
			// no longer the first item when we reach this point
			isFirstItem = false;
		}
		
		inputBuilder.append("}}");
		
		String output = inputBuilder.toString();
		
		log.info(String.format("Map to Schema Message - output: %s", output));
		
		return output;
    }
}
