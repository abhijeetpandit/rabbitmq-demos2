package routing;

import java.io.FileInputStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Random;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.AMQP.Queue.DeclareOk;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class MyTesting {
	public static final String QUEUE_PREFIX = "distQueue";
	public static final String EXCHANGE_NAME = "testEX2";
	public static final int MAX_THREADS = 4;
	private static final String msg="[\r\n" + 
			"  '{{repeat(5, 7)}}',\r\n" + 
			"  {\r\n" + 
			"    _id: '{{objectId()}}',\r\n" + 
			"    index: '{{index()}}',\r\n" + 
			"    guid: '{{guid()}}',\r\n" + 
			"    isActive: '{{bool()}}',\r\n" + 
			"    balance: '{{floating(1000, 4000, 2, \"$0,0.00\")}}',\r\n" + 
			"    picture: 'http://placehold.it/32x32',\r\n" + 
			"    age: '{{integer(20, 40)}}',\r\n" + 
			"    eyeColor: '{{random(\"blue\", \"brown\", \"green\")}}',\r\n" + 
			"    name: '{{firstName()}} {{surname()}}',\r\n" + 
			"    gender: '{{gender()}}',\r\n" + 
			"    company: '{{company().toUpperCase()}}',\r\n" + 
			"    email: '{{email()}}',\r\n" + 
			"    phone: '+1 {{phone()}}',\r\n" + 
			"    address: '{{integer(100, 999)}} {{street()}}, {{city()}}, {{state()}}, {{integer(100, 10000)}}',\r\n" + 
			"    about: '{{lorem(1, \"paragraphs\")}}',\r\n" + 
			"    registered: '{{date(new Date(2014, 0, 1), new Date(), \"YYYY-MM-ddThh:mm:ss Z\")}}',\r\n" + 
			"    latitude: '{{floating(-90.000001, 90)}}',\r\n" + 
			"    longitude: '{{floating(-180.000001, 180)}}',\r\n" + 
			"    tags: [\r\n" + 
			"      '{{repeat(7)}}',\r\n" + 
			"      '{{lorem(1, \"words\")}}'\r\n" + 
			"    ],\r\n" + 
			"    friends: [\r\n" + 
			"      '{{repeat(3)}}',\r\n" + 
			"      {\r\n" + 
			"        id: '{{index()}}',\r\n" + 
			"        name: '{{firstName()}} {{surname()}}'\r\n" + 
			"      }\r\n" + 
			"    ],\r\n" + 
			"    greeting: function (tags) {\r\n" + 
			"      return 'Hello, ' + this.name + '! You have ' + tags.integer(1, 10) + ' unread messages.';\r\n" + 
			"    },\r\n" + 
			"    favoriteFruit: function (tags) {\r\n" + 
			"      var fruits = ['apple', 'banana', 'strawberry'];\r\n" + 
			"      return fruits[tags.integer(0, fruits.length - 1)];\r\n" + 
			"    }\r\n" + 
			"  }\r\n" + 
			"]";

	public static void main(String[] args) throws Exception {
		ConnectionFactory factory = new ConnectionFactory();
		Properties props = new Properties();
		props.load(new FileInputStream("rabbitmq.properties"));
		factory.setHost(props.getProperty("url"));
		factory.setUsername(props.getProperty("userName"));
		factory.setPassword(props.getProperty("password"));
		System.out.println("Using props : " + props);
        Random r = new Random();
        try (Connection connection = factory.newConnection(); Channel channel = connection.createChannel()) {
        	channel.exchangeDeclare(EXCHANGE_NAME, "direct", true);
        	//channel.confirmSelect();
        	
        	for(int count = 0; count < MAX_THREADS; count++) {
        		String queueName = QUEUE_PREFIX + count;
        		DeclareOk declareOk = channel.queueDeclare(queueName, true, false, false, null);
        		channel.queueBind(queueName, EXCHANGE_NAME, String.valueOf(count));
        	}
			String dt = new SimpleDateFormat("dd HH:mm:ss").format(new Date());
			long currentTime = System.currentTimeMillis();
			channel.txSelect();
			for (int count = 1; count < 5000; count++) {
				
				String partition = String.valueOf(r.nextInt(4));
				String message = dt + "Message no " + count + msg;

				channel.basicPublish(EXCHANGE_NAME, partition, true, new AMQP.BasicProperties.Builder()
			               .contentType("text/plain")
			               .deliveryMode(2)
			               .priority(1)
			               .userId("admin")
			               .build(), message.getBytes("UTF-8"));
				
				//System.out.println(" [x] Sent '" + "partition =" + partition + "':'" + message + "'" );
			}
			channel.txCommit();
			System.out.println("Time to send  = " + (System.currentTimeMillis() - currentTime));
        }
	}
}
