package main.java.ca.yzhuin.paytm.utils;

import main.java.ca.yzhuin.paytm.objects.HitInfo;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.csv.QuoteMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

public class WebLogUtils
{
    //timestamp elb client:port backend:port request_processing_time backend_processing_time response_processing_time elb_status_code backend_status_code received_bytes sent_bytes "request" "user_agent" ssl_cipher ssl_protocol
    private static final Logger LOG = LoggerFactory.getLogger(WebLogUtils.class);

    private static final int TIMESTAMP = 0;
    private static final int CLIENT_IP = 2;
    private static final int REQUEST = 11;
    private static final int USER_AGENT = 12;

    public static HitInfo parse(String input)
    {
        CSVFormat csvFormat = CSVFormat.newFormat(' ').withQuote('"').withQuoteMode(QuoteMode.MINIMAL);
        CSVParser csvParser;
        HitInfo hitInfo = null;
        try
        {
            csvParser = CSVParser.parse(input, csvFormat);
            // there should be only one record
            for (CSVRecord record : csvParser)
            {
                String timestamp = record.get(TIMESTAMP);
                String clientIP = record.get(CLIENT_IP).split(":")[0];
                String request = record.get(REQUEST);
                String userAgent = record.get(USER_AGENT);
                // some simple check on the request
                // GET https://.... HTTP/1.1
                String[] requestSplitted = request.split("\\s+");
                String url;
                Instant instant = Instant.parse(timestamp);
                if (requestSplitted.length != 3)
                {
                    LOG.error("input string {} has incorrect request", input);
                    url = null;
                }
                else
                {
                    url = requestSplitted[1];
                }
                hitInfo = new HitInfo(instant, clientIP, url);
            }
        }
        catch (IOException e)
        {
           LOG.error("input string {} has io exception", input);
        }
        finally
        {
            return hitInfo;
        }
    }
}
