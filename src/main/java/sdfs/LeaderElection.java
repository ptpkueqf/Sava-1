package sdfs;

import org.apache.log4j.Logger;

public class LeaderElection {
    public static Logger logger = Logger.getLogger(LeaderElection.class);
    private static String Leader1 = "172.22.147.96";
    String LeaderIp = Leader1;


    public void run(){
        //TODO
    }

    // get the current Leader IP
    public String getLeaderIp(){
        return LeaderIp;
    }
}
