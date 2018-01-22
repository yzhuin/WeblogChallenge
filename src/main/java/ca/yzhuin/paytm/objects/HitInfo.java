package main.java.ca.yzhuin.paytm.objects;

import main.java.ca.yzhuin.paytm.utils.WebLogUtils;

import java.io.Serializable;
import java.time.Instant;

public class HitInfo implements Serializable, Comparable<HitInfo>
{
    private Instant timestamp;
    private String clientIP;
    private String url;

    public HitInfo(Instant timestamp, String clientIP, String url)
    {
        this.timestamp = timestamp;
        this.clientIP = clientIP;
        this.url = url;
    }

    public static HitInfo fromString(String input)
    {
        return WebLogUtils.parse(input);
    }

    public Instant getTimestamp()
    {
        return timestamp;
    }

    public void setTimestamp(Instant timestamp)
    {
        this.timestamp = timestamp;
    }

    public String getClientIP()
    {
        return clientIP;
    }

    public void setClientIP(String clientIP)
    {
        this.clientIP = clientIP;
    }

    public String getUrl()
    {
        return url;
    }

    public void setUrl(String url)
    {
        this.url = url;
    }

    public int compareTo(HitInfo that)
    {
        return this.timestamp.compareTo(that.timestamp);
    }

    @Override
    public String toString()
    {
        return clientIP + ","
                + timestamp + ","
                + url + ","
                + userAgent;
    }
}
