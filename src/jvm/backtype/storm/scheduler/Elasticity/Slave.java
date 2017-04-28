package backtype.storm.scheduler.Elasticity;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;


public class Slave {
	public static Profile prf;
	
	public static void main (String[] args) throws InterruptedException, IOException{
	//public static void start () throws UnknownHostException, InterruptedException{
        System.out.println("In main...");

	java.net.InetAddress localMachine = java.net.InetAddress.getLocalHost();




		prf=new Profile(localMachine.getCanonicalHostName());
       
		Thread t=new Thread(new ProfileUpdate());
		System.out.println("Machine canonical host name is: " + localMachine.getCanonicalHostName());
		System.out.println("Machine hostName (same as profile constructor) is: " + localMachine.getHostName());
		System.out.println("Starting profile update thread");
		t.start();
		while(true){
			Thread.sleep(2000);
			SlaveWorker worker = new SlaveWorker(args[0]);
			worker.run();
		}
	}

}

class SlaveWorker implements Runnable{

	private String masterIp;

	@Override
	public void run() {
		// TODO Auto-generated method stub
		
		try {
			//GAIN service info
			
			//prf.examine();
			//send service info
            Socket socket = new Socket(this.masterIp,6789);
			ObjectOutputStream out=new ObjectOutputStream(socket.getOutputStream());
			out.flush();
			out.writeObject(Slave.prf.hostname);
			out.writeObject(Slave.prf.getCpu_usage());
			//out.writeObject(Slave.prf.getBandwidth_in());
			//out.writeObject(Slave.prf.getBandwidth_out());
			out.flush();

        } catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public SlaveWorker(String ip) {
		this.masterIp = ip;
	}
	
}
