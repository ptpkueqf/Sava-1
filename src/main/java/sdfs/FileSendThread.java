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

/*** This class is used to send the file message to other nodes.
 * *
 */
public class FileSendThread extends Thread{
    public static Logger logger = Logger.getLogger(FileOperation.class);
    public static String[] sendMessage;
    private static int fileTransferPort = 4444;
    LeaderElection leader = new LeaderElection();
    String leaderIp = leader.getLeaderIp();
    int leaderPort =  leader.getLeaderPort();
    FileSendThread fst = new FileSendThread();
    public ArrayList<String> queryForIps(String[] message){
        //TODO
        Socket socket;
        boolean done;

        ArrayList<String> returnIps = new ArrayList<String>();
        try {
            socket = new Socket(leaderIp,fileTransferPort);
            InputStream inputs = socket.getInputStream();
            OutputStream outputs = socket.getOutputStream();
            //Sending message to the server
            DataOutputStream dataos = new DataOutputStream(outputs);
            for(String m : message) {
                dataos.writeUTF(m);
            }
            ObjectInputStream objectis = new ObjectInputStream(inputs);
            done = true;

            while(done)
            {
                try {
                    Object readObject = objectis.readObject();
                    returnIps = (ArrayList<String>)readObject;
                    if(returnIps.size()>0)
                    {
                        done = false;
                    }
                }
                catch (ClassNotFoundException e)
                {
                    done = false;
                    logger.info("File doesn't exist in the system");
                }

            }
        } catch (IOException e) {
            logger.info(e);
        }

        return returnIps;

    }





}
