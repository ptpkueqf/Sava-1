package sdfs;

import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.io.IOException;
import java.net.Socket;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.ObjectInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;


public class FileOperation {
    public static Logger logger = Logger.getLogger(FileOperation.class);
    public static String[] sendMessage;
    FileSendThread fst = new FileSendThread();




    // send putfile request to the leader and get the ips for operation
    public void putFile(String localfilename, String sdfsfilename) {
        ArrayList<String> ips;
        sendMessage[0] = "put";
        sendMessage[1] = localfilename;
        sendMessage[2] = sdfsfilename;
        ips = fst.queryForIps(sendMessage);

        for(String ip:ips) {
            sendMessage[0] = "send";
            FileReceiveThread ftc = new FileReceiveThread(ip, sendMessage);
        }


    }
    // send getfile request to the leader and get the ips for operation
    public void getFile(String localfilename, String sdfsfilename){
        ArrayList<String> ips;
        sendMessage[0] = "get";
        sendMessage[1] = localfilename;
        sendMessage[2] = sdfsfilename;
        ips = fst.queryForIps(sendMessage);
        for(String ip:ips) {
            sendMessage[0] = "receive";
            FileReceiveThread ftc = new FileReceiveThread(ip, sendMessage);
        }
    }
    // send deletefile request to the leader and get the ips for operation
    public void deleteFile(String sdfsfilename){
        ArrayList<String> ips;
        sendMessage[0] = "delete";
        sendMessage[1] = sdfsfilename;
        ips = fst.queryForIps(sendMessage);
        for(String ip:ips) {
            sendMessage[0] = "remove";
            FileReceiveThread ftc = new FileReceiveThread(ip, sendMessage);
        }
    }
    // query the leader for listing all addresses storing the file and return addresses
    public ArrayList<String> listMembers(String sdfsfilename){
        ArrayList<String> ips;
        sendMessage[0] = "listmembers";
        sendMessage[1] = sdfsfilename;
        ips =fst.queryForIps(sendMessage);
        return ips;
    }
    // listall files storing in this machine and return file names
    public String[] listFiles(String machineId){
        String[] fileNames = null;
        //TODO
        return fileNames;
    }

}
