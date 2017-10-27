package sdfs;


import grep.GrepClient;
import grep.GrepServer;
import membership.MemberGroup;
import org.apache.log4j.Logger;

import java.io.*;
import java.lang.reflect.*;
import java.util.ArrayList;


/***
 * This is the entry of the distributed file system, where members choose to
 *   put/get/delete files in the system.
 */

public class SystemMain {
    public static Logger logger = Logger.getLogger(SystemMain.class);
    private static String localFileName;
    private static String sdfsFileName;



    public static void main(String[] args) {

        SystemMain systemMain = new SystemMain();

        //start the grep server for grep query
        new GrepServer("8090").start();

        // first join the membership group
        System.out.println("\n Joining the membership group.");
        buildGroup();
        System.out.println(("\n Now you can enter the file system."));

        // ask the member to choose its action
        System.out.println("\nYou can choose the following action: ");
        System.out.println("\nEnter 'put localfilename sdfsfilename' to insert or update the file");
        System.out.println("\nEnter 'get sdfsfilename localfilename' to get the file from the SDFS");
        System.out.println("\nEnter 'delete sdfsfilename' to delete the file from the SDFS");
        System.out.println("\nEnter 'ls sdfsfilename' to list all members storing this file");
        System.out.println("\nEnter 'store sdfsfilename' to list all files storing in this member");
        System.out.println("\nEnter 'membership' to modify the membership group");
        System.out.println("\nEnter 'grep' and queries to grep\n");

        while(true) {
            InputStreamReader is_reader = new InputStreamReader(System.in);
            String memberActionline = "";
            try {
                memberActionline = new BufferedReader(is_reader).readLine();
            } catch (IOException e) {
                logger.error(e);
                e.printStackTrace();
            }
            String[] memberAction = memberActionline.split(" ");

            // act according to member's action
            if(memberAction[0].equalsIgnoreCase("put"))
            {
                systemMain.putFile(memberAction[1],memberAction[2]);
            }
            else if(memberAction[0].equalsIgnoreCase("get"))
            {
                systemMain.getFile(memberAction[2],memberAction[1]);
            }
            else if(memberAction[0].equalsIgnoreCase("delete"))
            {
                systemMain.deleteFile(memberAction[1]);
            }
            else if(memberAction[0].equalsIgnoreCase("ls"))
            {
                systemMain.listMember(memberAction[1]);
            }else if(memberAction[0].equalsIgnoreCase("store"))
            {
                systemMain.listFiles();
            }
            else if(memberAction[0].equalsIgnoreCase("membership"))
            {
                String idStr = "";
                try {
                    Method method = MemberGroup.class.getMethod("main",
                            String[].class);
                    method.invoke(null,
                            (Object) new String[] { idStr });
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }else if (memberAction[0].equalsIgnoreCase("grep"))
            {
                GrepClient.grep(memberAction);
            }else
            {
                System.out.println("wrong operation!  please input put, get, delete, ls, store, membership or grep command!");
                logger.info("wrong operation!  please input put, get, delete, ls, store, membership or grep command!");
            }
        }
    }
    /**
     * send join request to the introducer
     */
    public  static void buildGroup(){
        new MemberGroup().joinGroup();
    }

    /**
     * send put request to the master
     */
    public void putFile(String localfilename, String sdfsfilename){
        localFileName = localfilename;
        sdfsFileName = sdfsfilename;
        FileOperation put = new FileOperation();
        put.putFile(localFileName,sdfsFileName);
    }

    /**
     * send get request to the master
     */
    public void getFile(String localfilename, String sdfsfilename){
        localFileName = localfilename;
        sdfsFileName = sdfsfilename;
        FileOperation get = new FileOperation();
        get.getFile(localFileName,sdfsFileName);
    }
    /**
     * send delete request to the master
     */
    public void deleteFile(String sdfsfilename){
        sdfsFileName = sdfsfilename;
        FileOperation delete = new FileOperation();
        delete.deleteFile(sdfsFileName);

    }
    /**
     * send listmember request to the master and list the address
     * where this file is currently being stored
     */
    public void listMember(String sdfsfilename){
        sdfsFileName = sdfsfilename;
        FileOperation list = new FileOperation();
        //TODO
        ArrayList<String> addresses = list.listMembers(sdfsFileName);
        System.out.println("File Name" + sdfsFileName + "is currently storing at addresses\n" + addresses);
    }
    /**
     * send listFiles request to the master and
     * list the files currently being stored at this machine
     */
    public void listFiles(){
        String machineIp = MemberGroup.machineIp;
        FileOperation list = new FileOperation();
        String[] files = list.listFiles(machineIp);
        //TODO
        System.out.println("The following files are stored at this machine:\n");
        System.out.println(files);
    }


}
