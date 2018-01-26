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

public class AggSessionDriver
{
    private static final Logger LOG = LoggerFactory.getLogger(AggSessionDriver.class);

    public static void main(String[] args) throws Exception
    {
        String appName = MethodHandles.lookup().lookupClass().getName();
        SparkConf conf = new SparkConf().setAppName(appName);
        JavaSparkContext sc = new JavaSparkContext(conf);
        String inputPath = args[0];
        sc.setLogLevel(args[1]);

        // sessionize
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
        // get all the avgs
        List<Long> avgs = agg.map( tuple2 -> tuple2._2().getAverageSessionTime()).collect();
        for (long avg : avgs)
        {
           sum += avg;
        }
        long average = sum / avgs.size();

        System.out.println("Average session time: " + Duration.ofNanos(average));

        // top 10 users with long sessions, distinct url hits and total url hits are collected
        List<Tuple2<String, AggSessions>> top = agg.top(10, new SessionWindowComparator());
        for (Tuple2<String, AggSessions> t2 : top)
        {
            //print them
           // System.out.println("top engaged users and their session information:");
           // System.out.println(t2._2());
        }

        // full sessionized data
        List<Tuple2<String, AggSessions>> all = agg.collect();
        for (Tuple2<String, AggSessions> t2: all)
        {
            System.out.println(t2._2().printAll());
        }

        // for machine learning
        /*
        List<Tuple2<String, AggSessions>> all = agg.collect();

        for (Tuple2<String, AggSessions> t2 : all)
        {
            System.out.println(t2._2().printForML());
        }
        */
    }
}
