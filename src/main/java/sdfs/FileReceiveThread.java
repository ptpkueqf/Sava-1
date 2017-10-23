package sdfs;

import org.apache.log4j.Logger;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.ConcurrentHashMap;


/*** This class is used to receive the file message from other nodes.
 * *
 */

public class FileReceiveThread extends Thread {
    public static Logger logger = Logger.getLogger(FileReceiveThread.class);

    public static ConcurrentHashMap<String, FileInfo> leaderfileList = new ConcurrentHashMap<String, FileInfo>();

    LeaderElection leader = new LeaderElection();
    String leaderIp = leader.getLeaderIp();
    String machineIp;

    public void run(){
        try {
            machineIp = InetAddress.getLocalHost().getHostAddress().toString();
        } catch (UnknownHostException e) {
            logger.error(e);
            e.printStackTrace();
        }

    }



}
