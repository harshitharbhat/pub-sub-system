import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class Broker {
    static Map<String, Set<Integer>> topicToSubscriberMap = new ConcurrentHashMap<>();

    public static void addSubscriber(String topic, String subscriberPort) {
        System.out.println("Port " + subscriberPort + " just subscribed to '" + topic + "'");
        Set<Integer> set = topicToSubscriberMap.getOrDefault(topic, new HashSet<>());
        set.add(Integer.valueOf(subscriberPort));
        topicToSubscriberMap.put(topic, set);
    }

    public static void main(String[] args) {
        int port = Integer.parseInt(args[0]);
        try {
            ServerSocket curSocket = new ServerSocket(port);
            System.out.println("Broker running on port: " + port);
            while (true) {
                Socket socket = curSocket.accept();
                Thread t = new RequestHandler(socket, port, topicToSubscriberMap);
                t.start();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}

class RequestHandler extends Thread {

    final Socket socket;
    int currentBrokerPort;
    Map<String, Set<Integer>> topicToSubscriberPortsMap;

    public RequestHandler(Socket publisherSocket, int currentBrokerPort, Map<String,
            Set<Integer>> topicToSubscriberPortsMap) {
        socket = publisherSocket;
        this.currentBrokerPort = currentBrokerPort;
        this.topicToSubscriberPortsMap = topicToSubscriberPortsMap;
    }

    private void sendToSubscribers(String topic, String data) {
        if (data.isEmpty() || !topicToSubscriberPortsMap.containsKey(topic)) return;
        System.out.println("Publishing to '" + topic + "': " + data);

        try {
            for (int port : topicToSubscriberPortsMap.get(topic)) {
                System.out.println(port);
                Socket s = new Socket(InetAddress.getLocalHost(), port);
                DataInputStream dis = new DataInputStream(s.getInputStream());
                DataOutputStream dos = new DataOutputStream(s.getOutputStream());
                dos.writeUTF(topic + " " + data);

                dis.close();
                dos.close();
                s.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void run() {
        try {
            DataInputStream dis = new DataInputStream(socket.getInputStream());
            String data = dis.readUTF();
            String[] splitData = data.split(" ");
            String concatData = concatenateData(splitData);

            switch (splitData[0]) {
                case "pub":
                    sendToSubscribers(splitData[1], concatData);
                    break;
                case "add-sub":
                    Broker.addSubscriber(splitData[1], splitData[2]);
                    break;
                case "health-check":
                    DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
                    dos.writeUTF("OK");
                    dos.close();
                    break;
            }

            dis.close();
            socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static String concatenateData(String[] parts) {
        StringBuilder stringBuilder = new StringBuilder();
        for(int i = 2; i < parts.length; i++){
            stringBuilder.append(parts[i] + " ");
        }
        System.out.println(stringBuilder);
        return stringBuilder.toString();
    }
}