package membership;

import sdfs.SDFS;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;

public class NFailureDetect extends Thread{

    public void run() {
        long currenttime = System.currentTimeMillis();
        //System.out.println("Failure detection working!");
        for (Map.Entry<String, Node> entry : SDFS.alivelist.entrySet()) {
            try {
                if (entry.getKey().equals(InetAddress.getLocalHost().getHostAddress().toString())) {
                    continue;
                }
            }catch (UnknownHostException e) {
                e.printStackTrace();
            }
            if (currenttime - entry.getValue().lastime > 2500) {
                entry.getValue().isActive = false;
            }
        }
    }
}
