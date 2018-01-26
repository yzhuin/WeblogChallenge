package main.java.ca.yzhuin.paytm.driver;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.lang.invoke.MethodHandles;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;

public class AggLoadDriver
{
    private static final Logger LOG = LoggerFactory.getLogger(AggSessionDriver.class);

    public static void main(String[] args)
    {
        String appName = MethodHandles.lookup().lookupClass().getName();
        SparkConf conf = new SparkConf().setAppName(appName);
        JavaSparkContext sc = new JavaSparkContext(conf);
        String inputPath = args[0];

        Map<Instant, Long> agg = sc.textFile(inputPath)
                .mapToPair(line -> {
                    String timestamp = line.split("\\s")[0];
                    Instant instant = Instant.parse(timestamp);
                    Instant truncatedToMin = instant.truncatedTo(ChronoUnit.MINUTES);
                    return new Tuple2<>(truncatedToMin, 1);
                })
                .countByKey();

        Set<Instant> sortedKeySet = new TreeSet<>(agg.keySet());
        // output
        List<Instant> keys = new ArrayList<>(sortedKeySet);
        // for padding
        Instant first = keys.get(0);
        Instant last = keys.get(keys.size() - 1);

        Instant iter = first;
        for (; iter.isBefore(last); iter = iter.plus(1l, ChronoUnit.MINUTES))
        {
            if (agg.containsKey(iter))
            {
                // output average load
                System.out.println(iter + "," + ((double)agg.get(iter)/60));
            }
            else
            {
               // pad 0, no requests at all
                System.out.println(iter + "," + 0);
            }
        }
    }
}
