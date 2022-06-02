import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Scanner;


public class Subscriber {
    private static int port;

    public static void main(String[] args) {
        port = Integer.parseInt(args[0]);
        System.out.println("Subscriber running at " + port);
        Thread thread = new Listener(port);
        thread.start();
        Subscriber subscriber = new Subscriber();
        subscriber.inputScanner();
    }

    public void inputScanner() {
        try {
            Scanner scn = new Scanner(System.in);
            while (true) {
                String toSend = scn.nextLine();
                String[] parts = toSend.split(" ");
                subscribeToTopic(parts[1], port);
                System.out.println("Subscribed to '" + parts[1] + "'!");
            }
        } catch (Exception e) {
            e.printStackTrace();
            inputScanner();
        }
    }

    private void subscribeToTopic(String topic, int port) throws IOException {
        InetAddress ip = InetAddress.getLocalHost();
        Socket socket = new Socket(ip, Constants.MANAGER_SERVER_PORT);
        DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
        dos.writeUTF("sub " + topic + " " + port);
        dos.close();
        socket.close();
    }
}

class Listener extends Thread {
    final int port;

    public Listener(int port) {
        this.port = port;
    }

    public void run() {
        try {
            ServerSocket serverSocket = new ServerSocket(port);
            while (true) {
                Socket s = serverSocket.accept();
                DataInputStream dis = new DataInputStream(s.getInputStream());
                String received = dis.readUTF();
                String[] parts = received.split(" ");
                System.out.println("New message from '" + parts[0] + "': " + concatenateData(parts));
                dis.close();
                s.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    private static String concatenateData(String[] parts) {
        StringBuilder stringBuilder = new StringBuilder();
        for(int i = 1; i < parts.length; i++){
            stringBuilder.append(parts[i] + " ");
        }
        return stringBuilder.toString();
    }
}