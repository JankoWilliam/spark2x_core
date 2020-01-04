package sparkStreaming;

import java.util.Random;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientFactory;
import org.apache.flume.event.EventBuilder;
import java.nio.charset.Charset;


public class RpcClientDemo {
	
	public static void main(String[] args) throws InterruptedException {
		MyRpcClientFacade client = new MyRpcClientFacade();
		// Initialize client with the remote Flume agent's host and port
		client.init("172.16.20.130", 41414);

		// Send 10 events to the remote Flume agent. That agent should be
		// configured to listen with an AvroSource.
		for (int i =0; i < 300; i++) {
            int number = new Random().nextInt(3);
            String sampleData ;

            if(number == 0){
                sampleData  = "Hello Flume! ERROR   " + i;
            }else if(number==1){
                sampleData  = "Hello Flume! INFO   " + i;
            }else {
                sampleData  = "Hello Flume! WARNING   " + i;
            }

			client.sendDataToFlume(sampleData);
			System.out.println("发送数据：" + sampleData);
			Thread.sleep(500);
		}

		client.cleanUp();
	}
}

class MyRpcClientFacade {
	private RpcClient client;
	private String hostname;
	private int port;

	public void init(String hostname, int port) {
		// Setup the RPC connection
		this.hostname = hostname;
		this.port = port;
		this.client = RpcClientFactory.getDefaultInstance(hostname, port);
		// Use the following method to create a thrift client (instead of the
		// above line):
		// this.client = RpcClientFactory.getThriftInstance(hostname, port);
	}

	public void sendDataToFlume(String data) {
		// Create a Flume Event object that encapsulates the sample data
		Event event = EventBuilder.withBody(data, Charset.forName("UTF-8"));

		// Send the event
		try {
			client.append(event);
		} catch (EventDeliveryException e) {
			// clean up and recreate the client
			client.close();
			client = null;
			client = RpcClientFactory.getDefaultInstance(hostname, port);
			// Use the following method to create a thrift client (instead of
			// the above line):
			// this.client = RpcClientFactory.getThriftInstance(hostname, port);
		}
	}

	public void cleanUp() {
		// Close the RPC connection
		client.close();
	}
}
