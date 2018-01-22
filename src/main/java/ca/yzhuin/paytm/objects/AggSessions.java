package main.java.ca.yzhuin.paytm.objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;

public class AggSessions implements Serializable
{

    private static final Logger LOG = LoggerFactory.getLogger(AggSessions.class);

    private Set<SessionInfo> sessions;
    //session window - 15 minutes
    public static final long SESSION_WINDOW = 15;

    private static final String  DEBUG_IP = "61.246.224.170";

    public AggSessions() {}

    public static AggSessions newInstance()
    {
        return new AggSessions();
    }

    // used by createCombiner in combineByKey
    public AggSessions fromHitInfo(HitInfo hitInfo)
    {
        // should be null
        if (sessions == null)
        {
            // empty session list
            SessionInfo sessionInfo = new SessionInfo(hitInfo);
            // use a tree set to ensure it's ordered
            sessions = new TreeSet<>();
            sessions.add(sessionInfo);
        }
        return this;
    }

    // used by mergeValue in combineByKey operation
    // aggregate in the same partition
    public AggSessions aggWithHitInfo(HitInfo hitInfo)
    {
        // try to find a session that overlaps with this hit
        boolean added = false;
        int i = 0;
        int size = sessions.size();
        List<SessionInfo> sessionList = new ArrayList<>(sessions);
        if (sessionList.get(0).getClientIP().equals(DEBUG_IP))
        {
            LOG.debug("aggWithHitInfo - this agg session:\n{}", printAll());
            LOG.debug("aggWithHitInfo - adding hit:\n {}\n", hitInfo.toString());
        }
        for (; i < size; i++)
        {
            SessionInfo sessionInfo = sessionList.get(i);
            if (sessionInfo.include(hitInfo.getTimestamp()))
            {
                // added
                // and add only once
                sessionInfo.add(hitInfo);
                added = true;
                break;
            }
        }
        // no session includes this hit
        if (!added)
        {

            if (sessionList.get(0).getClientIP().equals(DEBUG_IP))
            {
                LOG.debug("aggWithHitInfo - creating new session with hit:\n {}\n", hitInfo.toString());
            }
            SessionInfo sessionInfo = new SessionInfo(hitInfo);
            sessions.add(sessionInfo);
        }
        else
        {
            // we need to check if we can merge further
            // i.e., the added hit can stitch two sessions together
            // check if it's the last one
            if (sessionList.get(0).getClientIP().equals(DEBUG_IP))
            {
                LOG.debug("aggWithHitInfo - added to existing session with hit:\n {}\n", hitInfo.toString());
            }
            if (i != size - 1)
            {
                SessionInfo nextSession = sessionList.get(i + 1);
                SessionInfo newSession =sessionList.get(i);
                // try to merge the affected sessions
                if (newSession.needsMerge(nextSession))
                {
                    // overlaps with the next session too
                    newSession.merge(nextSession);
                    sessions.remove(nextSession);
                }
            }
        }

        if (sessionList.get(0).getClientIP().equals(DEBUG_IP))
        {
            LOG.debug("after adding hit:\n{}", this.printAll());
        }
        return this;
    }

    // mergeCombiners - aggregate in different partitions
    public AggSessions merge(AggSessions aggSessions)
    {
        // add the new ones in
        Set<SessionInfo> newSessions = new TreeSet<>();
        newSessions.addAll(sessions);
        newSessions.addAll(aggSessions.sessions);
        List<SessionInfo> sessionList = new ArrayList<>(newSessions);
        int i = 0;
        int size = sessionList.size();
        List<List<Integer>> mergeMarkers = new ArrayList<>();
        SessionInfo mergeStart = sessionList.get(0);
        // for debug
        if (mergeStart.getClientIP().equals(DEBUG_IP))
        {
            LOG.debug("this agg session:\n{}", printAll());
            LOG.debug("that agg session:\n{}", aggSessions.printAll());
        }
        Instant postMergeStartTime = mergeStart.startTime;
        Instant postMergeEndTime = mergeStart.endTime;
        // a top level flag
        boolean needsMerge = false;
        // linear time complexity
        while (i < size)
        {
            int j = i + 1;
            for (; j < size; j++)
            {
                SessionInfo next = sessionList.get(j);
                // we need to use the stat/end time to check if merge is needed
                if (next.needsMerge(postMergeStartTime, postMergeEndTime))
                {
                    needsMerge = true;
                    // this means a new merge
                    if (j == i+1)
                    {
                        ArrayList<Integer> mList = new ArrayList<>();
                        // add i and j
                        mList.add(i);
                        mList.add(j);
                        mergeMarkers.add(mList);
                    }
                    // this means merge to the previously merged session
                    else
                    {
                        List<Integer> mList = mergeMarkers.get(mergeMarkers.size() - 1);
                        mList.add(j);
                    }
                    // update time if merged
                    postMergeStartTime = next.startTime.isBefore(postMergeStartTime) ? next.startTime
                                                                                     : postMergeStartTime;
                    postMergeEndTime = next.endTime.isAfter(postMergeEndTime) ? next.endTime
                                                                              : postMergeEndTime;
                }
                else
                {
                    // move on
                    // update post merge time
                    postMergeStartTime = next.startTime;
                    postMergeEndTime = next.endTime;
                    break;
                }
            }
            i = j;
        }

        if (needsMerge)
        {
            for (List<Integer> mList : mergeMarkers)
            {
                int index = 0;
                // merge to the first one
                SessionInfo aggregated = sessionList.get(mList.get(index));
                for (index = 1; index < mList.size(); index++)
                {
                    SessionInfo s = sessionList.get(mList.get(index));
                    aggregated.merge(s);
                    newSessions.remove(s);
                }
            }
        }

        AggSessions newAggSessions = newInstance();
        newAggSessions.sessions = newSessions;
        // for debug
        if (mergeStart.getClientIP().equals(DEBUG_IP))
        {
            LOG.debug("after merge:\n{}", newAggSessions.printAll());
        }
        return newAggSessions;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        for (SessionInfo s : sessions)
        {
           sb.append(s.toString() + "\n");
        }
        return sb.toString();
    }

    public String printAll()
    {
       StringBuilder sb = new StringBuilder();
       sb.append("##############################---agg session start---##############################\n");
       for (SessionInfo s : sessions)
       {
           sb.append(s.printAll());
       }
        sb.append("##############################---agg session end---##############################\n");
       return sb.toString();
    }

    public Duration getMaxSessionTime()
    {
        Duration maxSessionTime = Duration.ZERO;
        for (SessionInfo s : sessions)
        {
            Duration sessionTime = s.getDuration();
            // update
            maxSessionTime = sessionTime.compareTo(maxSessionTime) > 0 ? sessionTime : maxSessionTime;
        }
        return maxSessionTime;
    }

    public long getAverageSessionTime()
    {
        long sum = 0l;
        for (SessionInfo s : sessions) {
           sum += s.getDuration().toNanos();
        }
        // I won't bother with the accuracy for now
        // nano seconds are good enough
        return sum/sessions.size();
    }


    public static class SessionInfo implements Serializable, Comparable<SessionInfo>
    {
        private Set<HitInfo> hits;
        private Instant startTime;
        private Instant endTime;
        private String clientIP;

        public SessionInfo (HitInfo hitInfo)
        {
            // ordered
            hits = new TreeSet<>();
            hits.add(hitInfo);
            startTime = hitInfo.getTimestamp();
            endTime = hitInfo.getTimestamp();
            clientIP = hitInfo.getClientIP();
        }

        public boolean include(Instant timestamp)
        {
            // falls in between this session
            if (timestamp.isBefore(endTime.plus(SESSION_WINDOW, ChronoUnit.MINUTES))
                    && timestamp.isAfter(startTime.minus(SESSION_WINDOW, ChronoUnit.MINUTES)))
                return true;
            return false;
        }

        public boolean needsMerge(SessionInfo sessionInfo)
        {
            return needsMerge(sessionInfo.startTime, sessionInfo.endTime);
        }

        public boolean needsMerge(Instant thatStartTime, Instant thatEndTime)
        {
            if (include(thatStartTime) || include(thatEndTime))
                return true;
            else
                return false;
        }

        public void add(HitInfo hitInfo)
        {
            Instant hitTimestamp = hitInfo.getTimestamp();
            if (include(hitTimestamp))
            {
                if (clientIP.equals(DEBUG_IP) && hits.contains(hitInfo))
                {
                    LOG.error("something went wrong, there is a duplicate hit {}\n", hitInfo);
                    LOG.error("contains: {}", printAll());
                }
                hits.add(hitInfo);
                // update starttime or endtime if necessary
                if (hitTimestamp.isBefore(startTime))
                    startTime = hitTimestamp;
                if (hitTimestamp.isAfter(endTime))
                    endTime = hitTimestamp;
                // added
            }
        }

        public void merge(SessionInfo sessionInfo)
        {
            Instant thatStartTime = sessionInfo.startTime;
            Instant thatEndTime = sessionInfo.endTime;
            // update hits
            hits.addAll(sessionInfo.hits);
            // update timestamps;
            if (thatStartTime.isBefore(startTime))
                startTime = thatStartTime;
            if (thatEndTime.isAfter(endTime))
                endTime = thatEndTime;
        }


        @Override
        public int compareTo(SessionInfo thatSession)
        {
            return startTime.compareTo(thatSession.startTime);
        }

        public int getTotalHits()
        {
            return hits.size();
        }

        public int getUniqueHits()
        {
            Set<String> urls = new HashSet<>();
            for (HitInfo hit : hits)
            {
                urls.add(hit.getUrl());
            }
            return urls.size();
        }

        public String getClientIP()
        {
            return clientIP;
        }

        public Duration getDuration()
        {
            return Duration.between(startTime, endTime);
        }

        @Override
        public String toString()
        {
            return clientIP + ","
                    + startTime + ","
                    + endTime + ","
                    + getDuration() + ","
                    + getTotalHits() + "," + getUniqueHits();
        }

        public String printAll()
        {
            StringBuilder sb = new StringBuilder();
            sb.append("*****************************---session start---****************************\n");
            for (HitInfo hit : hits)
            {
               sb.append(hit.toString() + "\n");
            }
            sb.append("*****************************---session end---****************************\n");
            return sb.toString();
        }
    }
}
