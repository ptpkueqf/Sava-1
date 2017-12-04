package sava;

import membership.Node;
import org.apache.log4j.Logger;
import sdfs.FileClientThread;
import sdfs.FileOperation;
import sdfs.SDFS;

import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.*;

public class Master implements Runnable{

    public static Logger logger = Logger.getLogger(Master.class);
    //port for receiving command from client
    public static int commandport = 8099;
    //port for transferring messages with workers
    public static int messageport = 8199;

    private String application;
    private String graphFile;
    private int sourceVertexID;

    private List<String> workers;

    private ServerSocket serverSocket;

    private Socket socket;

    private int superstep;

    private HashMap<Integer, Vertex> vertices;

    private HashMap<Integer, String> partition;

    private List<Message> messages;

    private String vertexClassName;

    public static boolean restartOrNot = false;

    public long startTime;

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

            startTime = System.currentTimeMillis();

            System.out.println("Number of vertices :" + vertices.size());

            //get all the alive workers
            workers = new ArrayList<String>();

            //new MemberGroup().listMembership();
            for (Map.Entry<String, Node> entry : SDFS.alivelist.entrySet()) {
                String member = entry.getKey();
                if (!member.equals(masterIP) && !member.equals(standbyMaster) && !member.equals(client)) {
                    workers.add(member);
                }
            }

            System.out.println("Number of workers :" + workers.size());

            //new MemberGroup().listMembership();
            //divide the graph to workers
            partition = partition(vertices, workers);

            Socket socketToStand = new Socket(standbyMaster, commandport);


            ObjectOutputStream objectOutputStream = new ObjectOutputStream(socketToStand.getOutputStream());
            objectOutputStream.writeUTF(vertexClassName);
            objectOutputStream.writeObject(vertices);
            objectOutputStream.flush();
//          objectOutputStream.writeObject(partition);
//          objectOutputStream.flush();
//          objectOutputStream.close();

            //construct the original messages
            messages = constructMessages(vertices);

            System.out.println("Number of total messages: " + messages.size() );

            doIterations();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     * control computing in each super step
     * @throws IOException
     */
    private void doIterations() {
        boolean noActivity = false;
        boolean restart = false;
        do {
            System.out.println("Start iterations!");

            //before sending messages to workers, check if any worker has failed
            List<String> currentWorkers = new ArrayList<String>();
            for (Map.Entry<String, Node> entry : SDFS.alivelist.entrySet()) {
                Node member = entry.getValue();
                if (member.getIsActive() && !member.getIP().equals(masterIP) && !member.getIP().equals(standbyMaster) && !member.getIP().equals(client)) {
                    currentWorkers.add(member.getIP());
                }
            }

            System.out.println("172.22.147.99 is " + SDFS.alivelist.get("172.22.147.99").isActive + " current: "+currentWorkers.size() + " workers:" + workers.size());

            if (currentWorkers.size() < workers.size()) {
                System.out.println("Workers size  :" + workers.size() + "  CurrentWorkers Size :" + currentWorkers.size());
                //some node has failed
                workers = currentWorkers;
                partition = partition(vertices, workers);
                messages = constructMessages(vertices);
                restart = true;

            }

            HashMap<String, List<Message>> messagePartition = reorganizeMessage(partition, messages);


            List<Socket> socketList = new ArrayList<Socket>();
            List<String> tempworkers = new ArrayList<String>();
            List<Integer> tempsize = new ArrayList<Integer>();

            //deliver messages to worker
            try {

                boolean flag = false;


                for (String worker : workers) {

                    List<Message> messagesToWorker = messagePartition.get(worker);
                    Socket socketTW = new Socket(worker, messageport);
                    OutputStream outputStream = socketTW.getOutputStream();
                    ObjectOutputStream objects = new ObjectOutputStream(outputStream);

                    System.out.println("size of messages send to worker " + worker + " is" + messagesToWorker.size());
                    tempworkers.add(worker);
                    tempsize.add(messagesToWorker.size());

                    if (restart) {
                        System.out.println("Write restart command");
                        objects.writeUTF("restart");
                        objects.flush();
                        flag = true;
                    } else {
                        System.out.println("Write vertexClassName");
                        objects.writeUTF(vertexClassName);
                        objects.flush();
                    }

                    objects.writeObject(messagesToWorker);
                    objects.flush();

                    socketList.add(socketTW);

//                    Thread sendThread = new Thread(new SendMessageThread(worker,restart, messagePartition, socketList));
//                    sendThread.start();
//                    try {
//                        sendThread.join();
//                    } catch (InterruptedException e) {
//                        e.printStackTrace();
//                    }
                }

                if (flag) {
                    restart = false;
                }
            } catch (IOException e) {

                System.out.println("Send message error! Restart");

                for (Socket socket : socketList) {
                    try {
                        socket.close();
                    } catch (IOException e1) {
                        System.out.println("close sockets");
                    }
                }
                e.printStackTrace();
                socketList.clear();
                restartOrNot = false;
                continue;
            }


            System.out.println("Start iterations!");
            System.out.println("172.22.147.99 is " + SDFS.alivelist.get("172.22.147.99").isActive + " current: "+currentWorkers.size() + " workers:" + workers.size());

            for (int i = 0 ; i < tempworkers.size(); i++) {
                try {
                    Thread.sleep(i * 100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                System.out.println("size of messages send to worker " + tempworkers.get(i) +" is" + tempsize.get(i));
                System.out.println("Write vertexClassName");
            }
            tempworkers.clear();
            tempsize.clear();



            //clear the message list for new messages
            messages.clear();
            //receive messages from all the workers
            try {
                receiveMessages(socketList);
            }catch (IOException e) {
                System.out.print("Receive message error! Restart");

                for (Socket socket : socketList) {
                    try {
                        socket.close();
                    } catch (IOException e1) {
                        System.out.println("close sockets error");
                    }
                }
                e.printStackTrace();
                socketList.clear();
                continue;

            }

            //when there are no new messages
            if (messages.size() == 0) {
                //end the calculation
                noActivity = true;

                try {
                    for (String worker : workers) {
                        Socket socketTW = new Socket(worker, messageport);
                        OutputStream outputStream = socketTW.getOutputStream();
                        ObjectOutputStream objects = new ObjectOutputStream(outputStream);
                        objects.writeUTF("finish");
                        objects.flush();
                        objects.close();
                        socketTW.close();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }

                System.out.println("Calculation has been down, the total time cost is : " + (System.currentTimeMillis() - startTime) / 1000 + " seconds");

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

//                for (String worker : workers) {
//                    FileOperation get = new FileOperation();
//                    get.getFile(worker+"-solution", worker + "-solution");
//                }


                /////////////////////////////////////////////////////////
                ArrayList<SolutionType> solutionlist = new ArrayList<SolutionType>();


                File file = null;
                try {
                    file = new File(FileClientThread.LOCALADDRESS + InetAddress.getLocalHost().getHostAddress().toString() + "-solution");
                } catch (UnknownHostException e) {
                    e.printStackTrace();
                }
                if (file.isFile() && file.exists()) {

                        try {
                            FileInputStream fis = new FileInputStream(file);
                            //Construct BufferedReader from InputStreamReader
                            BufferedReader br = new BufferedReader(new InputStreamReader(fis));

                            String line = null;
                            while ((line = br.readLine()) != null) {
                                if (line.startsWith("#")) {
                                    continue;
                                }
                                String[] nodes = line.split("\\s+");
                                int vertexID = Integer.parseInt(nodes[0]);
                                double rankValue = Double.parseDouble(nodes[1]);
                                solutionlist.add(new SolutionType(vertexID, rankValue));
                            }
                            br.close();
                        }catch (IOException e) {
                            e.printStackTrace();
                        }
                    } else {
                        System.out.println("Cannot find the file" + graphFile);
                    }

                try {
                    System.out.println("Organizing results:");
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                System.out.println("The final results are :");
                //Collections.sort(solutionlist);
                for (int i = 0; i < 25; i++) {
                    System.out.println(solutionlist.get(i));
                }
            }
        } while (!noActivity);
    }

    /**
     *
     */
    private class SolutionType implements Comparable {

        public double value;
        public int ID;

        public SolutionType(int ID, double value) {
            this.ID = ID;
            this.value = value;
        }

        public String toString() {
            return ID + "    " + value;
        }

        public int compareTo(Object obj) {
            SolutionType solutionType = (SolutionType) obj;
            return (int)((solutionType.value - this.value) * 10 );
        }
    }


    public static String masterIP = "172.22.147.96";
    public static String standbyMaster = "172.22.147.97";
    public static String client = "172.22.147.98";

    /**
     * receive all new messages from workers
     * @throws IOException
     */
    private void receiveMessages(List<Socket> socketList) throws IOException {
//        int count = 0;
//        int size = workers.size();
//        boolean flag = true;
//        ServerSocket ssocket;
//        ssocket = new ServerSocket(messageport);

//        while (flag) {

          for (Socket socket : socketList) {
              InputStream inputStream = socket.getInputStream();
              ObjectInputStream objectInputStream = new ObjectInputStream(inputStream);
              try {
                  List<Message> inMessages = (List<Message>) objectInputStream.readObject();
                  messages.addAll(inMessages);
              } catch (ClassNotFoundException e) {
                  e.printStackTrace();
              }

//            count++;
//            if (count == size) {
//                //when receive all the messages from workers
//                flag = false;
//            }
              socket.close();
//        }
          }
        //ssocket.close();
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
        String[] commands = message.split("\\s+");

        vertexClassName = commands[0];
        graphFile = commands[1];
        socket.close();
        serverSocket.close();
    }


    /**
     * read graph from file
     * @param fileaddress
     * @return HashMap<VertexID, List<Adjacent VertexIDs>>
     */
    private HashMap<Integer,Vertex> readGraph(String fileaddress) throws IOException {
        HashMap<Integer, Vertex> map = new HashMap<Integer, Vertex>();

        File file = new File(FileClientThread.LOCALADDRESS + graphFile);
        if (file.isFile() && file.exists()) {

            FileInputStream fis = new FileInputStream(file);
            //Construct BufferedReader from InputStreamReader
            BufferedReader br = new BufferedReader(new InputStreamReader(fis));

            String line = null;
            while ((line = br.readLine()) != null) {
                if (line.startsWith("#")) {
                    continue;
                }
                String[] nodes = line.split("\\s+");
                int node1 = Integer.parseInt(nodes[0]);
                int node2 = Integer.parseInt(nodes[1]);

                if (map.containsKey(node1)) {
                    map.get(node1).getOutVertex().add(node2);
                } else {
                    try {
                        Vertex newvertex = (Vertex) Class.forName("application." + vertexClassName).newInstance();
                        newvertex.setVertexID(node1);
                        newvertex.getOutVertex().add(node2);
                        map.put(node1, newvertex);
                    } catch (InstantiationException e) {
                        e.printStackTrace();
                    } catch (IllegalAccessException e) {
                        e.printStackTrace();
                    } catch (ClassNotFoundException e) {
                        e.printStackTrace();
                    }
                }

                if (map.containsKey(node2)) {
                    map.get(node2).getOutVertex().add(node1);
                } else {
                    try {
                        Vertex newvertex = (Vertex) Class.forName("application." + vertexClassName).newInstance();
                        newvertex.setVertexID(node2);
                        newvertex.getOutVertex().add(node1);
                        map.put(node2, newvertex);
                    } catch (InstantiationException e) {
                        e.printStackTrace();
                    } catch (IllegalAccessException e) {
                        e.printStackTrace();
                    } catch (ClassNotFoundException e) {
                        e.printStackTrace();
                    }
                }
            }
            br.close();
        } else {
            System.out.println("Cannot find the file" + graphFile);
        }

        return map;
    }

    /**
     * partition function, map the computing task to workers
     * @param vertices
     * @param workers
     * @return partition of graph according to workers
     */
    private HashMap<Integer, String> partition(HashMap<Integer, Vertex> vertices, List<String> workers) {

        int numberOfvertex = vertices.size();
        int numberofworkers = workers.size();
        int num = numberOfvertex / numberofworkers;
        int res = numberOfvertex % numberofworkers;

        HashMap<Integer, String> partition = new HashMap<Integer, String>();

        //List<Integer> vertexes = new ArrayList<Integer>();
        int index = 0;
        int count = 0;

        for (Map.Entry<Integer, Vertex> vertexEntry : vertices.entrySet()) {
            //vertexes.add(vertex.getVertexID());
            count++;
            if (res > 0) {
                if (count > num + 1) {
                    index++;
                    count = 1;
                    res--;
                }
            } else {
                if (count > num) {
                    index++;
                    count = 1;
                }
            }
            partition.put(vertexEntry.getKey(), workers.get(index));
        }
        //partition.put(workers.get(count), new ArrayList<Integer>(vertexes));
        return partition;
    }

    /**
     * for the first time, construct the messages according to original graph
     * @param vertices
     * @return constructed messages
     */
    private List<Message> constructMessages(HashMap<Integer, Vertex> vertices) {
        List<Message> messages = new ArrayList<Message>();
        for (Map.Entry<Integer, Vertex> vertexEntry : vertices.entrySet()) {
            List<Integer> destVertices = vertexEntry.getValue().getOutVertex();
            for (Integer outId : destVertices) {
                messages.add(new Message(vertexEntry.getKey(), outId, vertexEntry.getValue().getValue()));
               // System.out.print("[" + vertexEntry.getKey() + "," + outId + "," + vertexEntry.getValue().getValue() + "]");
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

        for (String worker : workers) {
            messagePartition.put(worker, new ArrayList<Message>());
        }

        for (Message message : messages) {
            String worker = partition.get(message.getDestVertexID());
            messagePartition.get(worker).add(message);
        }
        return messagePartition;
    }

    class SendMessageThread implements Runnable {
        private String worker;
        private boolean restart;
        private HashMap<String, List<Message>> messagePartition;
        private List<Socket>  socketList;

        public SendMessageThread(String worker, boolean restart, HashMap<String, List<Message>> messagePartition, List<Socket> socketList) {
            this.worker = worker;
            this.restart = restart;
            this.messagePartition = messagePartition;
            this.socketList = socketList;
        }

        public void run() {

            Socket socketTW = null;
            try {
                List<Message> messagesToWorker = messagePartition.get(worker);
                socketTW = new Socket(worker, messageport);
                OutputStream outputStream = socketTW.getOutputStream();
                ObjectOutputStream objects = new ObjectOutputStream(outputStream);

                System.out.println("size of messages send to worker " + worker + " is" + messagesToWorker.size());

                if (restart) {
                    System.out.println("Write restart command");
                    objects.writeUTF("restart");
                    objects.flush();
                } else {
                    System.out.println("Write vertexClassName");
                    objects.writeUTF(vertexClassName);
                    objects.flush();
                }

                objects.writeObject(messagesToWorker);
                objects.flush();
            } catch (IOException e) {
                e.printStackTrace();
                Master.restartOrNot = true;
            }

            socketList.add(socketTW);

        }
    }




}
