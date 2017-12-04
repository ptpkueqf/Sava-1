package sava;

import membership.MemberGroup;
import membership.MemberInfo;
import org.apache.log4j.Logger;
import sdfs.FileClientThread;
import sdfs.FileOperation;

import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.*;

public class Worker implements Runnable{

    public static Logger logger = Logger.getLogger(Worker.class);
    private HashMap<Integer, Vertex> vertices;
    private ServerSocket serverSocket;
    private Socket socket;
    private List<Message> inputMessages;
    private int superstep = 0;

    private String vertexClassName;

    private List<Message> newMessages;

    private List<Message> previousMessages;

    public void run(){

        //Socket newsocket = null;
        try {
            serverSocket = new ServerSocket(Master.messageport);
            //newsocket = new Socket(Master.masterIP, Master.messageport);
        } catch (IOException e) {
            e.printStackTrace();
        }


        boolean isMasterAlive = true;
        while (true) {
            try {

                String inputmess;
                Object readObject = null;
                try {
                    socket = serverSocket.accept();

                    InputStream inputStream = socket.getInputStream();

                    //for the first time, get the vertexClassName
                    ObjectInputStream objects = new ObjectInputStream(inputStream);
                    inputmess = objects.readUTF();

                    if (inputmess.equals("finish")) {
                        System.out.println("finish, write solution");

                        writeSolution(vertices);

//                        socket.close();
//                        serverSocket.close();

//                        Socket finalSocket;
//                        if (isMasterAlive) {
//                            finalSocket = new Socket(Master.masterIP, Master.messageport);
//
//                        } else {
//                            finalSocket = new Socket(Master.standbyMaster, Master.messageport);
//                        }
//
//                        ObjectOutputStream objectOutputStream = new ObjectOutputStream(finalSocket.getOutputStream());
//                        objectOutputStream.writeObject(vertices);
//                        objectOutputStream.flush();
//                        objectOutputStream.close();
//                        finalSocket.close();

                        break;
                    } else {
                        readObject = objects.readObject();
                        inputMessages = (List<Message>) readObject;
                    }


                } catch (IOException e) {
                    System.out.println("Receive error, restart the worker and wait for new messages");

                    e.printStackTrace();

                    try {
                        socket.close();
                    } catch (IOException e1) {
                        e1.printStackTrace();
                    }

                    checkMaster();

                    continue;
                }


                System.out.println("UTF : " + inputmess);
                System.out.println("get "+ inputMessages.size() + " messages from master");

                if (inputmess.equals("restart")) {
                    System.out.println("Restart and construct vertices!");
                    vertexClassName = "PageRankVertex";
                    vertices = constructVertices(inputMessages);
                }

                if (vertices == null) {
                    vertexClassName = inputmess;
                    vertices = constructVertices(inputMessages);
                }



                //do computing in every vertex and get new messages
                newMessages = compute(vertices, inputMessages);

//                for (Map.Entry<Integer, Vertex> vertexEntry : vertices.entrySet()) {
//                    System.out.print("[" +vertexEntry.getValue().getVertexID() + "," + vertexEntry.getValue().getValue() + "]");
//                }
//                System.out.println();

                System.out.println("Size of new messages : " + newMessages.size());


                for (Map.Entry<String, MemberInfo> entry : MemberGroup.membershipList.entrySet()) {
                    if (entry.getValue().getIp().equals(Master.masterIP)) {
                        isMasterAlive = entry.getValue().getIsActive();
                    }
                }

                System.out.println("Master is alive:" + isMasterAlive);


                /////这里还要额外核查测试一下
                try {
                    if (isMasterAlive) {
                        System.out.println("Master is alive, begin send");
                        //the size of newMessages decide whether do we need more iterations.
                        OutputStream outputStream = socket.getOutputStream();
                        ObjectOutputStream outObjects = new ObjectOutputStream(outputStream);
                        outObjects.writeObject(newMessages);
                        outObjects.flush();
                        outObjects.close();
                        System.out.println("Master is alive, end send");
                    } else {
                        //send data to
                        System.out.println("Master is dead, begin send");
                        OutputStream outstr = socket.getOutputStream();
                        ObjectOutputStream outObjs = new ObjectOutputStream(outstr);
                        outObjs.writeObject(newMessages);
                        outObjs.flush();
                        outObjs.close();
                        System.out.println("Master is dead, end send");
                    }
                } catch (IOException e) {
                    System.out.println("Send messages error, restart and waiting for new messages");

                    checkMaster();

                    System.out.println("Continue");
                    continue;
                }

                //in the end of each superstep, update the previousMessages
                previousMessages = newMessages;

                superstep += 1;
                try {
                    socket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }

            }  catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
        }
    }


    /**
     * check whether the master has failed, if so, send previous messages to stand by master
     */
    private void checkMaster() {
        boolean isMasterAlive = true;

        System.out.println("Begin checking master");

        for (Map.Entry<String, MemberInfo> entry : MemberGroup.membershipList.entrySet()) {
            if (entry.getValue().getIp().equals(Master.masterIP)) {
                isMasterAlive = entry.getValue().getIsActive();
            }
        }



        if (!isMasterAlive) {
            //if the master has failed, then, we send the messsages of last super step to stand by master
            System.out.println("Master is down, begin sending messages to stand by master");
            try {
                Socket socket = new Socket(Master.standbyMaster, Standby.onetimeport);
                ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
                objectOutputStream.writeObject(previousMessages);
                objectOutputStream.flush();
                objectOutputStream.close();
                socket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


    /**
     * construct the local version of the partition of the graph
     * @param messages
     * @return
     */
    private HashMap<Integer, Vertex> constructVertices(List<Message> messages) {
        HashMap<Integer, Vertex> vertices = new HashMap<Integer, Vertex>();

        for (Message message : messages) {
            if (vertices.containsKey(message.getDestVertexID())) {
                vertices.get(message.getDestVertexID()).getOutVertex().add(message.getSourceVertexID());
            } else {
                Vertex tempvertex;
                try {
                    tempvertex = (Vertex)Class.forName("application." + vertexClassName).newInstance();
                    tempvertex.setVertexID(message.getDestVertexID());
                    tempvertex.getOutVertex().add(message.getSourceVertexID());
                    vertices.put(message.getDestVertexID(), tempvertex);
                } catch (InstantiationException e) {
                    e.printStackTrace();
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                }
            }
        }
        return vertices;
    }

    /**
     * do computing, according to the overridden method compute of each vertex.
     * @param vertices
     * @param messages
     * @return
     */
    private List<Message> compute(HashMap<Integer, Vertex> vertices, List<Message> messages) {
        List<Message> newmessages = new ArrayList<Message>();

        for (Message msg : messages) {
            vertices.get(msg.getDestVertexID()).getInputMessages().add(msg);
        }

        for (Map.Entry<Integer, Vertex> vertexentry : vertices.entrySet()) {
            vertexentry.getValue().compute(this.superstep);
            newmessages.addAll(vertexentry.getValue().getOutputMessages());
        }

        return newmessages;
    }

    /**
     * write solution to SDFS
     */
    private void writeSolution(HashMap<Integer, Vertex> vertices) {

        try {
            //write local solution to local file system
            String localIP = InetAddress.getLocalHost().getHostAddress().toString();
            String localFileName = localIP + "-solution";
            File outputfile = new File(FileClientThread.LOCALADDRESS + localFileName);
            outputfile.createNewFile(); //if exists, do nothing
            FileOutputStream out = new FileOutputStream(outputfile);
            for (Map.Entry<Integer, Vertex> vertexEntry : vertices.entrySet()) {
                out.write(vertexEntry.getValue().toString().getBytes());
            }
            out.close();

            //upload the solution file to sdfs
            FileOperation put = new FileOperation();
            put.putFile(localFileName, localFileName);

        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
