package membership;

import java.io.Serializable;

public class Node implements Serializable{

    public String IP;
    public long lastime;
    public boolean isActive;

    public boolean getIsActive() {
        return isActive;
    }

    public String getIP() {
        return IP;
    }

    public Node(String IP, Long time, boolean isActive) {
        this.IP = IP;
        this.lastime = time;
        this.isActive = isActive;
    }
}
