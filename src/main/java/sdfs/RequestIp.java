package sdfs;

import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.io.IOException;
import java.net.Socket;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.ObjectInputStream;
import java.io.DataOutputStream;

/*** This class is used to send the file message to other nodes.
 * *
 */
public class RequestIp {
    public static Logger logger = Logger.getLogger(FileOperation.class);
    public static String[] sendMessage;

    public static int fileTransferPort = SDFSMain.socketPort;

    LeaderElection leader = new LeaderElection();

    String leaderIp = leader.getLeaderIp();
    //int leaderPort =  leader.getLeaderPort();
    RequestIp fst = new RequestIp();

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

            dataos.writeUTF("query_");
            for (int i = 0; i < message.length; i++) {
                dataos.writeUTF(message[i]);
                if (i != message.length - 1) {
                    dataos.writeUTF("_");
                }
            }
            dataos.flush();

            ObjectInputStream objects = new ObjectInputStream(inputs);

//            done = true;
//            while(done)
//            {
                try {
                    Object readObject = objects.readObject();
                    returnIps = (ArrayList<String>)readObject;
//                    if(returnIps.size()>0)
//                    {
//                        done = false;
//                    }
                }
                catch (ClassNotFoundException e)
                {
                    done = false;
                    logger.info("..........");
                }

                socket.close();

        } catch (IOException e) {
            logger.info(e);
        }
        return returnIps;
    }
}
