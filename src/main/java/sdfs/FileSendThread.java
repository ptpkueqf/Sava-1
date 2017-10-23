package sdfs;

import org.apache.log4j.Logger;

import java.net.InetAddress;
import java.net.UnknownHostException;

/*** This class is used to send the file message to other nodes.
 * *
 */
public class FileSendThread extends Thread{
    public static Logger logger = Logger.getLogger(FileSendThread.class);
    LeaderElection leader = new LeaderElection();
    String leaderIp = leader.getLeaderIp();

    public void run(){
        try {
            String machineIp = InetAddress.getLocalHost().getHostAddress().toString();
        } catch (UnknownHostException e) {
            logger.error(e);
            e.printStackTrace();
        }

    }
    public void putFile(String localfilename, String sdfsfilename){

    }
    public void getFile(String localfilename, String sdfsfilename){

    }
    public void deleteFile(String sdfsfilename){

    }
    public String[] listMembers(String sdfsfilename){
        String[] addresses = null;
        return addresses;
    }
    public String[] listFiles(String machineId){
        String[] fileNames = null;
        return fileNames;
    }
}
