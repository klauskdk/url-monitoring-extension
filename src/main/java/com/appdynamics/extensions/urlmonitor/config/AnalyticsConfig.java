package com.appdynamics.extensions.urlmonitor.config;

@SuppressWarnings("unused")
public class AnalyticsConfig {

	    private boolean publishToAnalytics = false;
	    private String analyticsApiUrl = "";  
		private String globalAccountName = "";  
		private String apiKey = "";
		private String analyticsSchemaName = "UrlMonitorSchema"; // default value

	    public boolean getPublishToAnalytics(){
			return this.publishToAnalytics;
		}
		
		public void setPublishToAnalytics(boolean value){
			this.publishToAnalytics = value;
		}
		
		public String getAnalyticsApiUrl(){
			return this.analyticsApiUrl;
		}
		
		public void setAnalyticsApiUrl(String value){
			this.analyticsApiUrl = value;
		}
		
		public String getGlobalAccountName(){
			return this.globalAccountName;
		}
		
		public void setGlobalAccountName(String value){
			this.globalAccountName = value;
		}
		
		public String getApiKey(){
			return this.apiKey;
		}
		
		public void setApiKey(String value){
			this.apiKey = value;
		}
		
		public String getAnalyticsSchemaName(){
			return this.analyticsSchemaName;
		}
		
		public void setAnalyticsSchemaName(String value){
			this.analyticsSchemaName = value;
		}

	    @Override
	    public String toString()
	    {
	        return "publishToAnalytics=" + publishToAnalytics +
	                ", analyticsApiUrl=" + analyticsApiUrl +
	                ", analyticsSchemaName=" + analyticsSchemaName;
	    }
	}
