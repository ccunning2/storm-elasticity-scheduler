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




		prf=new Profile(localMachine.getHostName());
       
		Thread t=new Thread(new ProfileUpdate());
        System.out.println("Starting profile update thread");
		t.start();
		while(true){
			Thread.sleep(1000);
			SlaveWorker worker = new SlaveWorker();
			worker.run();
		}
	}

}

class SlaveWorker implements Runnable{

	@Override
	public void run() {
		// TODO Auto-generated method stub
		
		try {
			//GAIN service info
			
			//prf.examine();
			//send service info
            System.out.println("In worker run, opening connection...");
            Socket socket = new Socket("10.71.104.210",6789);
			ObjectOutputStream out=new ObjectOutputStream(socket.getOutputStream());
			out.flush();
			out.writeObject(Slave.prf.ip);
			out.writeObject(Slave.prf.getCpu_usage());
			//out.writeObject(Slave.prf.getBandwidth_in());
			//out.writeObject(Slave.prf.getBandwidth_out());
			out.flush();
            System.out.println("Completed transmission!");
        } catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
}
