package sdfs;

import java.io.Serializable;

/***
 * This class is used to store information of files in the distributed file system
 */

public class FileInfo implements Serializable{

    private String sdfsFileName;
    private String localFileName;
    private String ip;
    private long lastUpdateTime;

    public FileInfo(String sdfsFileName, String localFileName, String ip,long lastUpdateTime)
    {
       this.ip = ip;
       this.lastUpdateTime = lastUpdateTime;
       this.localFileName = localFileName;
       this.sdfsFileName = sdfsFileName;
    }
    public String getIp() {
        return ip;
    }

    public String getLocalFileName(){ return localFileName; }

    public String getSdfsFileName(){ return sdfsFileName; }

    public long getLastUpateTime()
    {
        return lastUpdateTime;
    }

    public void setLastUpdateTime(long lastUpdateTime)
    {
        this.lastUpdateTime = lastUpdateTime;
    }

}
