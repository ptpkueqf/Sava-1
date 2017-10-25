package sdfs;

import org.apache.log4j.Logger;

import java.io.*;
import java.net.*;

/*** This class is used to send the file message to other nodes.
 * *
 */
public class FileSendThread extends Thread{
    public static Logger logger = Logger.getLogger(FileSendThread.class);
    public static String[] sendMessage;
    LeaderElection leader = new LeaderElection();
    String leaderIp = leader.getLeaderIp();
    int leaderPort =  leader.getLeaderPort();

    public void run(){

        //TODO
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

    public void sendMessageOp(String[] message){
        //TODO

        // if the current machine is leader, then it doesn't need to send message
        if(isLeader()){
            FileReceiveThread op = new FileReceiveThread();
            op.leaderOp(message);
        }else {
            // send message to the leader
            DatagramSocket socket = null;
            try {
                socket = new DatagramSocket();
                ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);


                objectOutputStream.writeObject(message);

                byte[] buffer = byteArrayOutputStream.toByteArray();
                int length = buffer.length;
                DatagramPacket datagramPacket = new DatagramPacket(buffer, length);
                datagramPacket.setAddress(InetAddress.getByName(leaderIp));
                datagramPacket.setPort(leaderPort);

                socket.send(datagramPacket);

            } catch (SocketException e) {
                e.printStackTrace();
                logger.error(e);
            } catch (IOException e) {
                e.printStackTrace();
                logger.error(e);
            }
        }
    }
    public String[] queryListOp(String[] message){
        //TODO
        String[] list = null;
        sendMessageOp(message);
        return list;
    }

}
