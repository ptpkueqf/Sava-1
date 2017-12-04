package membership;

import sdfs.SDFS;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.*;
import java.util.Map;

public class NSendThread extends Thread{

    public void run() {

        for (Map.Entry<String, Node> entry : SDFS.alivelist.entrySet()) {

            try {
                if (entry.getKey().equals(InetAddress.getLocalHost().getHostAddress().toString())) {
                    continue;
                }
            }catch (UnknownHostException e) {
                e.printStackTrace();
            }

            //System.out.println("send :" + entry.getKey());

            DatagramSocket socket = null;
            //String[] introducers = new Introducers().getIntroducers();
            try {
                socket = new DatagramSocket();
                ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);

                objectOutputStream.writeObject(entry.getValue());

                byte[] buffer = byteArrayOutputStream.toByteArray();
                int length = buffer.length;
                DatagramPacket datagramPacket = new DatagramPacket(buffer, length);
                datagramPacket.setAddress(InetAddress.getByName(entry.getKey()));
                datagramPacket.setPort(8911);
                //since udp is connectless, we send more the message more than once to reduce the influence of loss
//            int count = 3;
//            while(count > 0) {
                socket.send(datagramPacket);
//                count--;
//            }
            } catch (SocketException e1) {
                e1.printStackTrace();
            } catch (IOException e2) {
                e2.printStackTrace();
            }
        }
    }
}
