package sava;

import application.ApplicationEntrance;
import membership.MemberGroup;
import membership.MemberInfo;
import org.apache.log4j.Logger;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;

public class Master implements Runnable{

    public static Logger logger = Logger.getLogger(Master.class);
    //port for receiving command from client
    public static int commandport = 8099;
    //port for transferring messages with workers
    public static int messageport = 8199;
    //port for transferring messages with standby master
    public static int standyport = 8299;

    private String application;
    private String graphFile;
    private int sourceVertexID;

    private List<String> workers;

    private ServerSocket serverSocket;

    private Socket socket;

    private int superstep;

    private HashSet<Vertex> vertices;

    private HashMap<Integer, String> partition;

    private List<Message> messages;

    private String vertexClassName;

    public Master() {
        superstep = 0;
    }

    public void run() {

        System.out.println("Waiting for commands from client");
        try {

            //read command from client
            readCommand();
            //read graph files and do partition
            vertices = readGraph(graphFile);

            //get all the alive workers
            workers = new ArrayList<String>();
            for (Map.Entry<String, MemberInfo> entry : MemberGroup.membershipList.entrySet()) {
                MemberInfo member = entry.getValue();
                if (member.getIsActive() && !member.getIp().equals(masterIP) && !member.getIp().equals(standbyMaster) && !member.getIp().equals(client)) {
                    workers.add(member.getIp());
                }
            }

            //divide the graph to workers
            partition = partition(vertices, workers);

            Socket socketToStand = new Socket(standbyMaster, commandport);
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(socketToStand.getOutputStream());
            objectOutputStream.writeObject(vertices);
            objectOutputStream.flush();
            objectOutputStream.writeObject(partition);
            objectOutputStream.flush();
            objectOutputStream.close();

            //construct the original messages
            messages = constructMessages(vertices);

            doIterations();


        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     * control computing in each super step
     * @throws IOException
     */
    private void doIterations() throws IOException {
        boolean noActivity = false;
        boolean restart = false;
        do {

            HashMap<String, List<Message>> messagePartition = reorganizeMessage(partition, messages);

            //before sending messages to workers, check if any worker has failed
            List<String> currentWorkers = new ArrayList<String>();
            for (Map.Entry<String, MemberInfo> entry : MemberGroup.membershipList.entrySet()) {
                MemberInfo member = entry.getValue();
                if (member.getIsActive() && !member.getIp().equals(masterIP) && !member.getIp().equals(standbyMaster) && !member.getIp().equals(client)) {
                    currentWorkers.add(member.getIp());
                }
            }
            if (currentWorkers.size() < workers.size()) {
                //some node has failed
                workers = currentWorkers;
                partition = partition(vertices, workers);
                messages = constructMessages(vertices);
                restart = true;

            }


            //deliver messages to worker
            for (String worker : workers) {
                List<Message> messagesToWorker = messagePartition.get(worker);
                Socket socketTW = new Socket(worker, messageport);
                OutputStream outputStream = socketTW.getOutputStream();
                ObjectOutputStream objects = new ObjectOutputStream(outputStream);

                if (restart) {
                    objects.writeUTF("restart");
                    objects.flush();
                    restart = false;
                } else {
                    objects.writeUTF(vertexClassName);
                    objects.flush();
                }

                objects.writeObject(messagesToWorker);
                objects.flush();
                socketTW.close();
            }

            //clear the message list for new messages
            messages.clear();
            //receive messages from all the workers
            receiveMessages();
            //when there are no new messages
            if (messages.size() == 0) {
                //end the calculation
                noActivity = true;

            }
        } while (!noActivity);
    }


    public static String masterIP = "172.22.147.96";
    public static String standbyMaster = "172.22.147.97";
    public static String client = "172.22.147.98";

    /**
     * receive all new messages from workers
     * @throws IOException
     */
    private void receiveMessages() throws IOException {
        int count = 0;
        int size = workers.size();
        boolean flag = true;
        ServerSocket ssocket;
        ssocket = new ServerSocket(messageport);

        while (flag) {
            Socket socket = ssocket.accept();
            InputStream inputStream = socket.getInputStream();
            ObjectInputStream objectInputStream = new ObjectInputStream(inputStream);
            try {
                List<Message> inMessages = (List<Message>) objectInputStream.readObject();
                messages.addAll(inMessages);
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }

            count++;
            if (count == size) {
                //when receive all the messages from workers
                flag = false;
            }
            socket.close();
        }
    }

    /**
     * read command from client
     * @throws IOException
     */
    private void readCommand() throws IOException {
        serverSocket = new ServerSocket(commandport);
        socket = serverSocket.accept();
        DataInputStream inputCommmand = new DataInputStream(socket.getInputStream());

        String message = inputCommmand.readUTF();
        String[] commands = message.split(",");

        application = commands[0];
        vertexClassName = commands[1];
        socket.close();
        serverSocket.close();
    }


    /**
     * read graph from file
     * @param fileaddress
     * @return HashMap<VertexID, List<Adjacent VertexIDs>>
     */
    private HashSet<Vertex> readGraph(String fileaddress) {
        HashSet<Vertex> set = new HashSet<Vertex>();

        try {
            //create vertex object according to the vertexClassName.
            Vertex vertex = (Vertex)Class.forName(vertexClassName).newInstance();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return set;
    }

    /**
     * partition function, map the computing task to workers
     * @param vertices
     * @param workers
     * @return partition of graph according to workers
     */
    private HashMap<Integer, String> partition(HashSet<Vertex> vertices, List<String> workers) {

        int numberOfvertex = vertices.size();
        int numberofworkers = workers.size();
        int num = numberOfvertex / numberofworkers;

        HashMap<Integer, String> partition = new HashMap<Integer, String>();

        //List<Integer> vertexes = new ArrayList<Integer>();
        int index = 0;
        int count = 0;

        for (Vertex vertex : vertices) {
            //vertexes.add(vertex.getVertexID());
            count++;
            if (count > num + 1) {
                index++;
                count = 1;
            }
            partition.put(vertex.getVertexID(), workers.get(index));
        }
        //partition.put(workers.get(count), new ArrayList<Integer>(vertexes));
        return partition;
    }

    /**
     * for the first time, construct the messages according to original graph
     * @param vertices
     * @return constructed messages
     */
    private List<Message> constructMessages(HashSet<Vertex> vertices) {
        List<Message> messages = new ArrayList<Message>();
        for (Vertex vertex : vertices) {
            List<Integer> destVertices = vertex.getOutVertex();
            for (Integer outId : destVertices) {
                messages.add(new Message(vertex.getVertexID(), outId, vertex.getValue()));
            }
        }
        return messages;
    }

    /**
     * divide the message to target worker
     * @param partition
     * @param messages
     * @return
     */
    private HashMap<String, List<Message>> reorganizeMessage(HashMap<Integer, String> partition, List<Message> messages) {
        HashMap<String, List<Message>> messagePartition = new HashMap<String, List<Message>>();
        for (Message message : messages) {
            String worker = partition.get(message.getDestVertexID());

            if (messagePartition.containsKey(worker)) {
                messagePartition.get(worker).add(message);
            } else {
                List<Message> temp = new ArrayList<Message>();
                temp.add(message);
                messagePartition.put(worker, temp);
            }
        }
        return messagePartition;
    }

}
