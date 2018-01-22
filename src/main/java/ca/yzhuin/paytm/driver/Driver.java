package main.java.ca.yzhuin.paytm.driver;

import java.lang.invoke.MethodHandles;
import java.time.Duration;
import java.util.List;

import main.java.ca.yzhuin.paytm.objects.AggSessions;
import main.java.ca.yzhuin.paytm.objects.HitInfo;
import main.java.ca.yzhuin.paytm.utils.SessionWindowComparator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

public class Driver
{
    private static final Logger LOG = LoggerFactory.getLogger(Driver.class);

    public static void main(String[] args)
    {
        String appName = MethodHandles.lookup().lookupClass().getName();
        SparkConf conf = new SparkConf().setAppName(appName);
        JavaSparkContext sc = new JavaSparkContext(conf);
        String inputPath = args[0];
        sc.setLogLevel(args[1]);

        //JavaPairRDD<String, HitInfo> input =
        JavaPairRDD<String, AggSessions> agg = sc.textFile(inputPath)
            .mapToPair(line ->
            {
                HitInfo hitInfo = HitInfo.fromString(line);
                String key;
                if (hitInfo != null)
                {
                    key = hitInfo.getClientIP();
                }
                else
                {
                    // we'll just ignore it
                    key = "";
                    LOG.error("wrong url for line {}", line);
                }
                return new Tuple2<>(key, hitInfo);
            })
            // ignore
            .filter(tuple2 -> tuple2._2() != null)
            .combineByKey
            (
                // create combiner
                hitInfo -> AggSessions.newInstance().fromHitInfo(hitInfo),
                // combiner within same partition
                (aggSessions, hitInfo)-> aggSessions.aggWithHitInfo(hitInfo),
                // combiner on different partitions
                (aggSessionA, aggSessionB) -> aggSessionA.merge(aggSessionB)
            );
        agg.cache();

        // calc average session time
        long sum = 0l;
        List<Tuple2<String, AggSessions>> allAggregated = agg.collect();
        for (Tuple2<String, AggSessions> t2 : allAggregated)
        {
             sum += t2._2().getAverageSessionTime();
        }
        long average = sum / allAggregated.size();

        System.out.println("Average session time: " + Duration.ofNanos(average));

        // top 10 users with long sessions, distinct url hits and total url hits are collected
        // feel free to print them all to a file
        List<Tuple2<String, AggSessions>> top = agg.top(10, new SessionWindowComparator());
        for (Tuple2<String, AggSessions> t2 : top)
        {
           System.out.println(t2._2());
        }
    }
}
