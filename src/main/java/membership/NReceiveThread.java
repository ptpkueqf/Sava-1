package membership;

import sdfs.SDFS;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;

public class NReceiveThread extends Thread{

    public void run(){

        for (int i = 96; i <= 105; i++) {
            SDFS.alivelist.put("172.22.147."+i, new Node("172.22.147."+i, System.currentTimeMillis(), true));
        }

        while (true) {
            byte[] receiveBuffer = new byte[2048];
            try {
                DatagramSocket receiveSocket = new DatagramSocket(8911);
                DatagramPacket receivePacket = new DatagramPacket(receiveBuffer, receiveBuffer.length);
                receiveSocket.receive(receivePacket);
                //logger.info("Receive message from : " + receivePacket.getAddress());

                byte[] data = receivePacket.getData();
                ByteArrayInputStream bytestream = new ByteArrayInputStream(data);
                ObjectInputStream objInpStream = new ObjectInputStream(bytestream);
                Node message = (Node) objInpStream.readObject();

                String IP = receivePacket.getAddress().toString();
                IP = IP.substring(1);



                if (IP.contains(InetAddress.getLocalHost().getHostAddress().toString())) {
                    continue;
                }

                //System.out.println("//////////////////Receive heartbeat from " + IP);

                SDFS.alivelist.get(IP).isActive = true;
                SDFS.alivelist.get(IP).lastime = System.currentTimeMillis();

                receiveSocket.close();

            } catch (SocketException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
        }
    }

}
