package sdfs;

import membership.MemberGroup;
import membership.MemberInfo;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;


public class LeaderElection {
    public static Logger logger = Logger.getLogger(LeaderElection.class);
    private static String Leader1 = "172.22.147.96";
    private String Leader2 = "172.22.147.97";
    private String Leader3 = "172.22.147.98";


    // get the current Leader IP
    public String getLeaderIp(){
        Map<String, MemberInfo> maps = MemberGroup.membershipList;
        HashSet<String> aliveServers = new HashSet<String>();
        for (Map.Entry<String, MemberInfo> entry : maps.entrySet()) {
            if (entry.getValue().getIsActive()) {
                aliveServers.add(entry.getValue().getIp());
            }
        }
        if (aliveServers.contains(Leader1)) {
            return Leader1;
        } else if (aliveServers.contains(Leader2)) {
            return Leader2;
        } else if (aliveServers.contains(Leader3)){
            return Leader3;
        } else {
            return null;
        }
    }

    public ArrayList<String> getAliveLeaders() {
        Map<String, MemberInfo> maps = MemberGroup.membershipList;
        ArrayList<String> aliveLeaders = new ArrayList<String>();
        for (Map.Entry<String, MemberInfo> entry : maps.entrySet()) {
            if (entry.getValue().getIsActive()) {
                if (entry.getValue().getIp().equals(Leader1)) {
                    aliveLeaders.add(Leader1);
                } else if (entry.getValue().getIp().equals(Leader2)) {
                    aliveLeaders.add(Leader2);
                } else if (entry.getValue().getIp().equals(Leader3));
            }
        }
        return aliveLeaders;
    }


}
