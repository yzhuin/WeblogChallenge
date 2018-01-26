package main.java.ca.yzhuin.paytm.objects;

import main.java.ca.yzhuin.paytm.utils.WebLogUtils;

import java.io.Serializable;
import java.net.URL;
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
                + url;
    }

    public String printForML() throws Exception
    {
        URL u = new URL(url);
        // somehow %3F mixed in...
        String path = u.getPath().split("(?i)%3F")[0];
        String query = u.getQuery();
        // feature engineering - 00 - 23
        String hour = timestamp.toString().split("T")[1].split(":")[0];

        // feature engineering - ignore parameters
        return clientIP + "," + hour + "," + path + "," + getQueryFields(query);
    }

    public String getQueryFields(String query)
    {
        if (query == null)
            return null;
        StringBuilder sb = new StringBuilder();
        String[] pairs = query.split("&");
        for (String pair : pairs)
        {
            String[] splitted = pair.split("=");
            if (splitted.length <= 0)
                continue;
            else
                sb.append(splitted[0]);
        }
        return sb.toString();
    }

}
