package FilleSystem;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;


import Gossip.application;

public class FileListOperation {

	private Random rand;
	
	public FileListOperation()
	{
		rand = new Random();
	}
	private synchronized HashSet<String> getThreeNodes()
	{
		System.out.println("get three nodes called");
		int index;
		HashSet<String> selectedIp = new HashSet<String>();
		if(application.activeNodes.size()>=3)
		{
		//	index = rand.nextInt(application.activeNodes.size());
		do {			
		index = rand.nextInt(application.activeNodes.size());
		System.out.println("Reached inside the while loop for selecting the nodes");
		selectedIp.add(application.activeNodes.get(index).getAddress());
		}while(!(selectedIp.size() == 3));
		}
		else
		{
			System.out.println("Not enough nodes");
		}
		return selectedIp;
	}
	
	public synchronized HashSet<String> putInsideFileList(String fileName)
	{ 
		
		HashSet<String> listOfIpAddresses = getThreeNodes();
		System.out.println("reached inside filelist put function");;
		FileList newFileObj = new FileList(fileName,listOfIpAddresses);
		application.fileList.add(newFileObj);
		for(FileList fl:application.fileList)
		{
			System.out.println(fl.get_filename());
			System.out.println(fl.getStoreAddress());
		}
		System.out.println(application.fileList);
		return listOfIpAddresses;
	}
	
	public synchronized HashSet<String> getFromFileList(String fileName)
	{ 
		
		HashSet<String> listOfIpAddresses = new HashSet<String>();
		for(FileList fileObj:application.fileList)
		{
			if(fileObj.get_filename().equalsIgnoreCase(fileName))
			{
				listOfIpAddresses = fileObj.getStoreAddress();
			}
		}		
		return listOfIpAddresses;
	}
}
