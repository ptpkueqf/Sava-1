package sdfs;

import membership.MemberGroup;
import membership.MemberInfo;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;


public class LeaderElection {
    public static Logger logger = Logger.getLogger(LeaderElection.class);
//    private static String Leader1 = "172.22.147.96";
//    private String Leader2 = "172.22.147.97";
//    private String Leader3 = "172.22.147.98";

    private static String Leader1 = "10.194.193.97";
    private static String Leader2 = "10.195.9.90";
    private static String Leader3 = "";

    // get the current Leader IP
    public String getLeaderIp(){
        Map<String, MemberInfo> maps = MemberGroup.membershipList;
        HashSet<String> aliveServers = new HashSet<String>();
        System.out.println("maps size : " + maps.size());

        for (Map.Entry<String, MemberInfo> entry : maps.entrySet()) {

            if (entry.getValue().getIsActive()) {
                aliveServers.add(entry.getValue().getIp());
            }
        }

        System.out.println("out of loop");
        System.out.println("alive servers" + aliveServers);
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
