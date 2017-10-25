package sdfs;

import membership.MemberGroup;
import org.apache.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.*;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Random;


/*** This class is used to receive the file message from other nodes.
 * *
 */

public class FileReceiveThread extends Thread {
    public static Logger logger = Logger.getLogger(FileReceiveThread.class);
    public static int port = MemberGroup.receivePort;
    public String IP;
    private Random rand;

    public static ConcurrentHashMap<String, FileInfo> leaderFileList = new ConcurrentHashMap<String, FileInfo>();

    LeaderElection leader = new LeaderElection();
    String leaderIp = leader.getLeaderIp();
    String machineIp;
    String[] message;

    public void run(){
        try {
            machineIp = InetAddress.getLocalHost().getHostAddress().toString();
        } catch (UnknownHostException e) {
            logger.error(e);
            e.printStackTrace();
        }
        while (true) {

            // receive operation message
            byte[] receiveBuffer = new byte[2048];
            try {
                DatagramSocket receiveSocket = new DatagramSocket(port);
                DatagramPacket receivePacket = new DatagramPacket(receiveBuffer, receiveBuffer.length);
                receiveSocket.receive(receivePacket);

                byte[] data = receivePacket.getData();
                ByteArrayInputStream bytestream = new ByteArrayInputStream(data);
                ObjectInputStream objInpStream = new ObjectInputStream(bytestream);
                message = (String[]) objInpStream.readObject();

                IP = receivePacket.getAddress().toString();

                leaderOp(message);
                //read or write the files according to ips
                //TODO

                receiveSocket.close();

            } catch (SocketException e) {
                e.printStackTrace();
                logger.error(e);
            } catch (IOException e) {
                e.printStackTrace();
                logger.error(e);
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
                logger.error(e);
            }
        }

    }

    public ArrayList<String> leaderOp(String[] message){
        ArrayList<String> selectedIps = null;
        ArrayList<String> ips = null;
        int index;
         if(message[0].equalsIgnoreCase("put")){
             //check whether the file is already in the filelist
             if(checkExist(message[2])){
                 // return 3 random  ips
                 if(MemberGroup.membershipList.size()>=3)
                 {
                     do {
                         index = rand.nextInt(MemberGroup.membershipList.size());
                         selectedIps.add(MemberGroup.membershipList.get(index).getIp());
                     }while(!(selectedIps.size() == 3));
                 }
                 else
                 {
                     System.out.println("Not enough nodes");
                     logger.info("Not enough nodes");
                 }
             }else{
                 //return 2 of 3 ips for client to update
                 index = rand.nextInt(3);
                 for(Map.Entry<String, FileInfo> entry : leaderFileList.entrySet())
                 {
                     if(message[2] == entry.getKey()){
                         ips.add(entry.getValue().getIp());
                     }
                     while(selectedIps.size()<3){
                         selectedIps.add(ips.get(index));
                     }
                 }
             }
         }else if(message[0].equalsIgnoreCase("get")){
             //check whether the file is in the filelist
             if(checkExist(message[2])){
                 //return the 2 of 3 ips for client to get the file
                 index = rand.nextInt(3);
                 for(Map.Entry<String, FileInfo> entry : leaderFileList.entrySet())
                 {
                     if(message[2] == entry.getKey()){
                         ips.add(entry.getValue().getIp());
                     }
                     do{
                         selectedIps.add(ips.get(index));
                     }while(selectedIps.size()<3);
                 }
             }else{
                 System.out.println("The file doesn't exist!");
                 logger.info("The file doesn't exist!");
             }

         }else if(message[0].equalsIgnoreCase("delete")){
             //check whether the file is in the filelist
             if(checkExist(message[2])){
             //return 3 ips to delete the file
                 for(Map.Entry<String, FileInfo> entry : leaderFileList.entrySet())
                 {
                     if(message[2] == entry.getKey()){
                         selectedIps.add(entry.getValue().getIp());
                     }
                 }
             }else {
                 System.out.println("The file doesn't exist!");
                 logger.info("The file doesn't exist!");
             }
         }
         return selectedIps;
    }
    public boolean checkExist(String filename){
        for(Map.Entry<String, FileInfo> entry : leaderFileList.entrySet())
        {
            if(filename == entry.getKey()){
                return true;
            }

        }
        return false;
    }


}
