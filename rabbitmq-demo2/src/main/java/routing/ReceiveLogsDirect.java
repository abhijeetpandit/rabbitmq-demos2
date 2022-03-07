package routing;
import com.rabbitmq.client.*;

public class ReceiveLogsDirect {

  private static final String EXCHANGE_NAME = "testEx1";

  public static void main(String[] argvs) throws Exception {
    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost("localhost");
	factory.setUsername("admin");
	factory.setPassword("admin");
    Connection connection = factory.newConnection();
    Channel channel = connection.createChannel();

    channel.exchangeDeclare(EXCHANGE_NAME, "direct", true);

    String[] argv = new String[] {"0", "1", "2"};
    
    for (String partition : argv) {
        String queueName = "logQueue" + partition;
        channel.queueDeclare(queueName, true, false, false, null);
        channel.queueBind(queueName, EXCHANGE_NAME, partition);
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            try {
            	System.out.println("partition =" +partition+"->>>>>"+Thread.currentThread().getName() + "--->" + " [x] Received '" +
                        delivery.getEnvelope().getRoutingKey() + "':'" + message + "'");
            	if(partition.equals("0"))
            		Thread.sleep(10000);
            	else
            		Thread.sleep(10);
    		} catch (Exception e) {
    			e.printStackTrace();
    		}
        };
        channel.basicConsume(queueName, true, deliverCallback, consumerTag -> { });
    }
    System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

   
  }
}