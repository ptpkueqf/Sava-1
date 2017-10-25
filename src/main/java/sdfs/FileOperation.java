package sdfs;

import org.apache.log4j.Logger;

public class FileOperation {
    public static Logger logger = Logger.getLogger(FileOperation.class);
    public static String[] sendMessage;
    LeaderElection leader = new LeaderElection();
    String leaderIp = leader.getLeaderIp();
    int leaderPort =  leader.getLeaderPort();
    FileSendThread fst = new FileSendThread();

    // send putfile request to the leader and get the ips for operation
    public void putFile(String localfilename, String sdfsfilename) {
        //call leader's file operation
        //FileReceiveThread leaderPutFile = new FileReceiveThread();
        sendMessage[0] = "put";
        sendMessage[1] = localfilename;
        sendMessage[2] = sdfsfilename;
        fst.sendMessageOp(sendMessage);

    }
    // send getfile request to the leader and get the ips for operation
    public void getFile(String localfilename, String sdfsfilename){
        //call leader's file operation
        //FileReceiveThread leaderGetFile = new FileReceiveThread();
        sendMessage[0] = "get";
        sendMessage[1] = localfilename;
        sendMessage[2] = sdfsfilename;
        fst.sendMessageOp(sendMessage);
    }
    // send deletefile request to the leader and get the ips for operation
    public void deleteFile(String sdfsfilename){
        //call leader's file operation
        //FileReceiveThread leaderPutFile = new FileReceiveThread();
        sendMessage[0] = "delete";
        sendMessage[1] = sdfsfilename;
        fst.sendMessageOp(sendMessage);

    }
    // query the leader for listing all addresses storing the file and return addresses
    public String[] listMembers(String sdfsfilename){
        String[] addresses = null;
        //call leader's file operation
        //FileReceiveThread leaderPutFile = new FileReceiveThread();
        sendMessage[0] = "listmembers";
        sendMessage[1] = sdfsfilename;
        addresses =fst.queryListOp(sendMessage);
        return addresses;
    }
    // list all files storing in this machine and return file names
    public String[] listFiles(String machineId){
        String[] fileNames = null;
        // TODO
        return fileNames;
    }

}
