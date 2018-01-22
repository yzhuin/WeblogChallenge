package main.java.ca.yzhuin.paytm.utils;

import main.java.ca.yzhuin.paytm.objects.AggSessions;
import scala.Tuple2;

import java.io.Serializable;
import java.time.Duration;
import java.util.Comparator;

public class SessionWindowComparator implements Serializable, Comparator<Tuple2<String, AggSessions>>
{
    @Override
    public int compare(Tuple2<String, AggSessions> t1, Tuple2<String, AggSessions> t2)
    {
        Duration max1 = t1._2().getMaxSessionTime();
        Duration max2 = t2._2().getMaxSessionTime();
        return max1.compareTo(max2);
    }
}
