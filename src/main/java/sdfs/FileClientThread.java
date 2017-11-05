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

public class FileClientThread extends Thread {
    public static Logger logger = Logger.getLogger(FileClientThread.class);
    public static int port = SDFSMain.socketPort;
    public String ip;
    public Random rand;
    public String currentIp;
    public String[] message;
    private String LOCALADDRESS = "/home/shaowen2/mp2/localfolder/";



    LeaderElection leader = new LeaderElection();
    String leaderIp = leader.getLeaderIp();

    public FileClientThread(String ip, String[] message) {
        this.ip = ip;
        this.message = message;
        this.port = port;
    }

    public void run() {

//        try {
//            currentIp = InetAddress.getLocalHost().getHostAddress().toString();
//        } catch (UnknownHostException e) {
//            logger.error(e);
//            e.printStackTrace();
//        }
//        if(isLeader()){
//            leaderFileOperation(this.message);
//        }else {

        fileOperation(this.message);

//        }

    }
//    public void leaderFileOperation(String[] receivedmessage){
//        fileOperation(this.message);
//        if(receivedmessage[0].equalsIgnoreCase("put")||receivedmessage[0].equalsIgnoreCase("get")
//                ||receivedmessage[0].equalsIgnoreCase("delete")) {
//            leaderListOp(receivedmessage);
//        }
//    }

    public void fileOperation(String[] receivedmessage) {
        Socket socket = null;
        try {
            socket = new Socket(this.ip, this.port);
        } catch (IOException e) {
            logger.info(e);
        }

        if (receivedmessage[0].equalsIgnoreCase("put")) {
            try {
                InputStream inputs = socket.getInputStream();
                OutputStream outputs = socket.getOutputStream();
                DataOutputStream dataOps = new DataOutputStream(outputs);

                for (int i = 0; i < message.length; i++) {
                    dataOps.writeUTF(message[i]);
                    if (i != message.length - 1) {
                        dataOps.writeUTF("_");
                    }
                }

                dataOps.flush();

                File file = new File(LOCALADDRESS + this.message[1]);
                // turn file into byte
                byte[] bytefile = new byte[(int) file.length()];

                FileInputStream fileInput = new FileInputStream(file);
                BufferedInputStream bufferInput = new BufferedInputStream(fileInput);
                DataInputStream dataInput = new DataInputStream(bufferInput);

                dataInput.readFully(bytefile, 0, bytefile.length);
                dataOps.writeLong((long) bytefile.length);
                dataOps.write(bytefile, 0, bytefile.length);
                dataOps.flush();
                logger.info("Sent file :" + this.message[1] + "to" + this.ip);
                socket.close();
                return;
            } catch (IOException e) {
                logger.info(e);
            }
        } else if (receivedmessage[0].equalsIgnoreCase("get")) {
            byte[] receivedFile = new byte[1024];
            try {
                InputStream inputs = socket.getInputStream();
                OutputStream outputs = socket.getOutputStream();
                DataOutputStream dataOps = new DataOutputStream(outputs);
                for (int i = 0; i < message.length; i++) {
                    dataOps.writeUTF(message[i]);
                    if (i != message.length - 1) {
                        dataOps.writeUTF("_");
                    }
                }
                dataOps.flush();

                DataInputStream dataIps = new DataInputStream(inputs);
                FileOutputStream fileOps = new FileOutputStream(LOCALADDRESS + this.message[1]);

                int fileSize = dataIps.read(receivedFile, 0, (int) Math.min((long) receivedFile.length, dataIps.readLong()));

                fileOps.write(receivedFile, 0, fileSize);
                logger.info("File :" + this.message[1] + " received ");

            } catch (IOException e) {
                logger.info(e);
            }
        } else if (receivedmessage[0].equalsIgnoreCase("delete")) {
            try {
                OutputStream outputs = socket.getOutputStream();
                DataOutputStream dataOps = new DataOutputStream(outputs);
                for (int i = 0; i < message.length; i++) {
                    dataOps.writeUTF(message[i]);
                    if (i != message.length - 1) {
                        dataOps.writeUTF("_");
                    }
                }
                dataOps.flush();
            } catch (IOException e) {
                logger.info(e);
            }
        } else if (receivedmessage[0].equalsIgnoreCase("replicate")) {
            try {
                OutputStream outputs = socket.getOutputStream();
                DataOutputStream dataOps = new DataOutputStream(outputs);
                for (int i = 0; i < message.length; i++) {
                    dataOps.writeUTF(message[i]);
                    if (i != message.length - 1) {
                        dataOps.writeUTF("_");
                    }
                }
                dataOps.flush();
            } catch (IOException e) {
                logger.info(e);
            }
        }
    }


}
