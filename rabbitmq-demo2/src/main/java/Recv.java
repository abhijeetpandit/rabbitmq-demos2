import java.io.IOException;
import java.nio.charset.StandardCharsets;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

public class Recv {

    private final static String QUEUE_NAME = "queue2";
/*added comment*/
    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("172.19.1.167");
		factory.setUsername("insync");
        factory.setPassword("admin123");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        channel.basicQos(1);
        channel.queueDeclare(QUEUE_NAME, true, false, false, null);
        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        extracted(channel, "aa");
    }

	private static void extracted(Channel channel, final String name) throws IOException {
		DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            System.out.println(name+" [x] Received '" + message + "'");
            try {
            	System.out.println(Thread.currentThread().getName());
            	Thread.sleep(1);
			} catch (Exception e) {
				e.printStackTrace();
			}
            
        };
        channel.basicConsume(QUEUE_NAME, true, deliverCallback, consumerTag -> { });
	}
}