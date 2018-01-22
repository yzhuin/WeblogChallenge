package test.java;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.csv.QuoteMode;

import java.io.File;
import java.io.FileReader;
import java.time.Instant;

public class ParserTest
{
    public static void main(String[] args) throws Exception
    {
        //parse("C:\\Users\\Yuefei\\Development\\job_hunting\\interviews\\PaytmLabs\\WeblogChallenge\\data\\2015_07_22_mktplace_shop_web_log_sample.log");
        Instant instant = Instant.parse("2015-07-22T16:10:38.028609Z");
        System.out.println("nano: " + instant.getNano());
        System.out.println("milli: " + instant.getEpochSecond());
        System.out.println("echpo nano: " + instant.getEpochSecond() * 1000000000 + instant.getNano());
    }

    // I used this to check if there is a line has quote char in side quotes
    // there are some like line 491258
    // 2015-07-22T16:10:38.028609Z marketpalce-shop 106.51.132.54:4841 10.0.4.227:80 0.000022 0.000989 0.00002 400 400 0 166 "GET https://paytm.com:443/'"\'\");|]*{%0d%0a<%00>/about/ HTTP/1.1" "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0)" DHE-RSA-AES128-SHA TLSv1
    public static void parse(String file) throws Exception
    {
        CSVFormat csvFormat = CSVFormat.newFormat(' ').withQuote('"').withQuoteMode(QuoteMode.MINIMAL);
        CSVParser csvParser = csvFormat.parse(new FileReader(new File(file)));
        for (CSVRecord record : csvParser)
        {

        }
    }
}
