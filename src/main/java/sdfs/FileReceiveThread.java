package sdfs;

import membership.MemberGroup;
import org.apache.log4j.Logger;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.BufferedInputStream;
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
    public String ip;
    public Random rand;
    public String currentIp;
    public String[] message;

    public static ConcurrentHashMap<String, FileInfo> leaderFileList = new ConcurrentHashMap<String, FileInfo>();

    LeaderElection leader = new LeaderElection();
    String leaderIp = leader.getLeaderIp();


    public FileReceiveThread(String ip, String[] message) {
        this.ip = ip;
        this.message = message;
        this.port = port;
    }

    public void run(){

        try {
            currentIp = InetAddress.getLocalHost().getHostAddress().toString();
        } catch (UnknownHostException e) {
            logger.error(e);
            e.printStackTrace();
        }
        if(isLeader()){
            leaderFileOperation(this.message);
        }else {

            fileOperation(this.message);
        }

    }
    public void leaderFileOperation(String[] receivedmessage){
        fileOperation(this.message);
        if(receivedmessage[0].equalsIgnoreCase("put")||receivedmessage[0].equalsIgnoreCase("get")
                ||receivedmessage[0].equalsIgnoreCase("delete")) {
            leaderListOp(receivedmessage);
        }
    }
    public void fileOperation(String[] receivedmessage){
        Socket socket = null;
        try {
            socket = new Socket(this.ip, this.port);
        } catch (IOException e) {
            logger.info(e);
        }

        if(receivedmessage[0].equalsIgnoreCase("send")){
            try{
                InputStream inputs = socket.getInputStream();
                OutputStream outputs = socket.getOutputStream();
                DataOutputStream dataOps = new DataOutputStream(outputs);
                for(String m : this.message) {
                    dataOps.writeUTF(m);
                }
                dataOps.flush();
                File file= new File("/home//MP3/localfolder/" + this.message[1]);
                // turn file into byte
                byte[] bytefile = new byte[(int)file.length()];

                FileInputStream fileInput = new FileInputStream(file);
                BufferedInputStream bufferInput = new BufferedInputStream(fileInput);
                DataInputStream dataInput = new DataInputStream(bufferInput);

                dataInput.readFully(bytefile, 0, bytefile.length);
                dataOps.writeLong((long)bytefile.length);
                dataOps.write(bytefile, 0, bytefile.length);
                dataOps.flush();
                logger.info("Sent file :"+this.message[1]+"to"+this.ip);
                socket.close();
                return;
            }catch (IOException e) {
                logger.info(e);
            }
            } else if(receivedmessage[0].equalsIgnoreCase("receive")){
            byte[] receivedFile = new byte[1024];
            try{
                InputStream inputs = socket.getInputStream();
                OutputStream outputs = socket.getOutputStream();
                DataOutputStream dataOps = new DataOutputStream(outputs);
                for(String m : this.message) {
                    dataOps.writeUTF(m);
                }
                dataOps.flush();

                DataInputStream dataIps = new DataInputStream(inputs);
                FileOutputStream fileOps = new FileOutputStream("/home/MP3/localfolder/" + this.message[1]);

                int fileSize = dataIps.read(receivedFile,0,(int)Math.min((long)receivedFile.length, dataIps.readLong()));

                fileOps.write(receivedFile,0,fileSize);
               logger.info("File :" + this.message[1] + " received ");

            }catch (IOException e) {
                logger.info(e);
            }

            }else if(receivedmessage[0].equalsIgnoreCase("remove")){
                try{
                    OutputStream outputs = socket.getOutputStream();
                    DataOutputStream dataOps = new DataOutputStream(outputs);
                    for(String m : this.message) {
                        dataOps.writeUTF(m);
                    }
                    dataOps.flush();
                }catch (IOException e){
                    logger.info(e);
                }

            }
    }

    public ArrayList<String> leaderListOp(String[] message){
        //TODO only return the ips but not edit the file list yet
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
    // check whether the current is master
    public boolean isLeader(){
        try {
            String machineIp = InetAddress.getLocalHost().getHostAddress().toString();
            if(machineIp == leaderIp ){
                return true;
            }
        } catch (UnknownHostException e) {
            logger.error(e);
            e.printStackTrace();
        }
        return false;
    }


}
