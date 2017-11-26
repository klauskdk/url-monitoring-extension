package com.appdynamics.extensions.urlmonitor.config;

@SuppressWarnings("unused")
public class MonitorConfig
{
    private ClientConfig clientConfig;
    private AnalyticsConfig analyticsConfig;
    private DefaultSiteConfig defaultParams;
    private SiteConfig[] sites;
    private String metricPrefix;

    public ClientConfig getClientConfig()
    {
        return clientConfig;
    }

    public void setClientConfig(ClientConfig clientConfig)
    {
        this.clientConfig = clientConfig;
    }
    
    public AnalyticsConfig getAnalyticsConfig()
    {
    	return analyticsConfig;
    }
    
    public void setAnalyticsConfig(AnalyticsConfig analyticsConfig)
    {
    	this.analyticsConfig = analyticsConfig;
    }

    public DefaultSiteConfig getDefaultParams()
    {
        return defaultParams;
    }

    public void setDefaultParams(DefaultSiteConfig defaultParams)
    {
        this.defaultParams = defaultParams;
    }

    public SiteConfig[] getSites()
    {
        return sites;
    }

    public void setSites(SiteConfig[] sites)
    {
        this.sites = sites;
    }

    public int getTotalAttemptCount()
    {
        int total = 0;
        for (SiteConfig site : sites)
        {
            if (site.getNumAttempts() == -1) {
                total += getDefaultParams().getNumAttempts();
            } else {
                total += site.getNumAttempts();
            }
        }

        return total;
    }

    public String getMetricPrefix() {
        return metricPrefix;
    }

    public void setMetricPrefix(String metricPrefix) {
        this.metricPrefix = metricPrefix;
    }
}
