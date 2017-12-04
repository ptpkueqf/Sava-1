package application;

import org.apache.log4j.Logger;
import sava.Master;
import sava.Standby;
import sava.Worker;
import sdfs.SDFS;

import java.io.*;
import java.net.Socket;

/**
 * program entrance
 */
public class ApplicationEntrance {

    public static Logger logger = Logger.getLogger(ApplicationEntrance.class);
    public static String masterIP = "172.22.147.96";


    public static void main(String[] args) {
        ApplicationEntrance save = new ApplicationEntrance();
        save.start(args[0]);

     }

    private void start(String type) {

        //construct the file system
        constructSystem();

        if (type.equalsIgnoreCase("master")) {
            startMaster();
        } else if (type.equalsIgnoreCase("standby")) {
            startStandby();
        } else if (type.equalsIgnoreCase("client")) {
            startClient();
        } else if (type.equalsIgnoreCase("worker")) {
            startWorker();
        } else {
            System.out.println("Wrong type! Please reenter");
        }
    }

    private void constructSystem() {
        SDFS sdfs = new SDFS();
        sdfs.start();
    }

    private void startMaster() {
        Thread masterthread = new Thread(new Master());
        masterthread.start();
    }

    private void startStandby() {
        Thread standbythread = new Thread(new Standby());
        standbythread.start();

    }

    private void startWorker() {
        Thread workerthread = new Thread(new Worker());
        workerthread.start();
    }

    private void startClient() {
        System.out.println("Successfully enter the client");
        System.out.println("Please input the application name, file address and essential inform!");

        InputStreamReader is_reader = new InputStreamReader(System.in);
        String memberActionline = "";
        try {
            memberActionline = new BufferedReader(is_reader).readLine();
        } catch (IOException e) {
            logger.error(e);
            e.printStackTrace();
        }

        Socket socket = null;
        try {
            socket = new Socket(masterIP, Master.commandport);
            OutputStream outputs = socket.getOutputStream();
            //Sending command to the master
            DataOutputStream dataos = new DataOutputStream(outputs);
            String UTF = memberActionline;
            dataos.writeUTF(UTF);
            dataos.flush();

            InputStream input = socket.getInputStream();
            System.out.println("Computing");


        } catch (IOException e) {
            e.printStackTrace();
        }


    }
}
