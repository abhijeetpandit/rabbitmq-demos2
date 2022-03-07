package routing;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;

public class EmitLog {

	private static final String EXCHANGE_NAME = "testEx1";

	public static void main(String[] argv) throws Exception {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost("localhost");
		factory.setUsername("admin");
		factory.setPassword("admin");
		Random r = new Random();

		try (Connection connection = factory.newConnection(); Channel channel = connection.createChannel()) {
			channel.exchangeDeclare(EXCHANGE_NAME, "direct", true);
			String dt = new SimpleDateFormat("dd HH:mm:ss").format(new Date());
			for (int count = 1; count < 20; count++) {
				String partition = String.valueOf(r.nextInt(3));
				String message = dt + "Message no " + count;

				channel.basicPublish(EXCHANGE_NAME, partition, MessageProperties.PERSISTENT_TEXT_PLAIN, message.getBytes("UTF-8"));
				System.out.println(" [x] Sent '" + "partition =" + partition + "':'" + message + "'");
				Thread.sleep(10);
			}
		}
	}
	// ..
}