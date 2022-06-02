import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class BrokerManager {

    static void publishToLeader(int leaderPort, String topic, String data) throws IOException {
        InetAddress ip = InetAddress.getLocalHost();
        Socket s = new Socket(ip, leaderPort);
        DataOutputStream dos = new DataOutputStream(s.getOutputStream());
        dos.writeUTF("pub " + topic + " " + data);
        dos.close();
        s.close();
        System.out.println("Published data to leader at: " + leaderPort + ", topic: " + topic + ", data: " + data);
    }

    static void addSubscriberToAllBrokers(List<Integer> brokerPorts, String topic, int subscriberPort) throws IOException {
        InetAddress ip = InetAddress.getLocalHost();
        for (int port : brokerPorts) {
            Socket s = new Socket(ip, port);
            DataOutputStream dos = new DataOutputStream(s.getOutputStream());
            dos.writeUTF("add-sub " + topic + " " + subscriberPort);
            dos.close();
            s.close();
        }

        System.out.println("Added subscriber (port " + subscriberPort + ") for topic '" + topic + "'");
    }

    public static void main(String[] args) throws UnknownHostException {
        InetAddress ip = InetAddress.getLocalHost();
        ServerSocket serverSocket = null;
        HeartBeat thread = null;

        try {
            serverSocket = new ServerSocket(Constants.MANAGER_SERVER_PORT);
            System.out.println("Manager server running on port " + Constants.MANAGER_SERVER_PORT);

            thread = new HeartBeat(Constants.BROKER_1_PORT);
            thread.start();
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }

        while (true) {
            Socket socket = null;
            try {
                socket = serverSocket.accept();
                System.out.println("Request received from socket: " + socket);
                DataInputStream dis = new DataInputStream(socket.getInputStream());
                DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
                String data = dis.readUTF();
                String[] parts = data.split(" ");
                String publishedData = concatenateData(parts);

                if (!data.isEmpty() && parts.length >= 3) {
                    switch (parts[0]) {
                        case "pub":
                            publishToLeader(thread.getCurrentLeaderPort(), parts[1], publishedData);
                            break;

                        case "sub":
                            addSubscriberToAllBrokers(thread.getActiveBrokerPorts(), parts[1], Integer.valueOf(parts[2]));
                            break;
                    }
                }
                dis.close();
                dos.close();
                socket.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private static String concatenateData(String[] parts) {
        StringBuilder stringBuilder = new StringBuilder();
        for(int i = 2; i < parts.length; i++){
            stringBuilder.append(parts[i] + " ");
        }
        return stringBuilder.toString();
    }
}

class HeartBeat extends Thread {

    static ConcurrentHashMap<Integer, Integer> idToBrokerPortMap = new ConcurrentHashMap<>();
    static ConcurrentHashMap<Integer, Boolean> brokerPortToStatusMap = new ConcurrentHashMap<>();
    static ArrayList<Integer> brokerPorts = new ArrayList<>(Arrays.asList(
            Constants.BROKER_1_PORT,
            Constants.BROKER_2_PORT,
            Constants.BROKER_3_PORT
    ));

    int leaderPort;
    int currentBrokerId;

    public HeartBeat(int leaderPort) {
        this.leaderPort = leaderPort;
        initMaps();
    }

    static void initMaps() {
        int processId = 1;
        for (int port : brokerPorts) {
            idToBrokerPortMap.put(processId++, port);
            brokerPortToStatusMap.put(port, true);
        }
    }

    public int getCurrentLeaderPort() {
        return leaderPort;
    }

    public List<Integer> getActiveBrokerPorts() {
        List<Integer> ports = new ArrayList<>();
        for (Map.Entry<Integer, Boolean> entry : brokerPortToStatusMap.entrySet()) {
            if (entry.getValue()) ports.add(entry.getKey());
        }
        return ports;
    }

    public void run() {
        while (true) {
            Socket brokerSocket;
            // regularly check on all the brokers to watch for
            // 1. brokers going offline
            // 2. brokers coming back online
            for (int port : brokerPorts) {
                try {
                    brokerSocket = new Socket(InetAddress.getLocalHost(), port);
                    DataInputStream dis = new DataInputStream(brokerSocket.getInputStream());
                    DataOutputStream dos = new DataOutputStream(brokerSocket.getOutputStream());
                    dos.writeUTF("health-check");
                    String response = dis.readUTF();

                    if (response.equals("OK")) {
                        brokerPortToStatusMap.put(leaderPort, true);
                    }
                    dis.close();
                    dos.close();
                    brokerSocket.close();
                } catch (IOException e) {
                    brokerPortToStatusMap.put(leaderPort, false);
                    if (port == leaderPort) {
                        System.out.println("Leader is down!");
                        leaderPort = idToBrokerPortMap.get(electLeader());
                    }
                }
            }

            System.out.println("Heartbeat check succeeded");
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public int electLeader() {
        System.out.println("Leader election initiated...");
        int highestProcess = 1;

        for (int processId : idToBrokerPortMap.keySet()) {
//            if (processId > currentBrokerId) {
                int destServer = idToBrokerPortMap.get(processId);
                if (brokerPortToStatusMap.get(destServer)) {
                    highestProcess = Math.max(highestProcess, processId);
                }
//            }
        }
        System.out.println("New leader elected: " + highestProcess);
        return highestProcess;
    }
}