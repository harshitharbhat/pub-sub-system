import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.util.Scanner;

public class Publisher {

    public static void main(String[] args) {
        System.out.println("Publisher running ...");
        Publisher publisher = new Publisher();
        publisher.inputScanner();
    }

    public void inputScanner() {
        try {
            Scanner scn = new Scanner(System.in);
            while (true) {
                String toSend = scn.nextLine();
                publishData(toSend);
                Thread.sleep(1000);
            }
        } catch (Exception e) {
            e.printStackTrace();
            inputScanner();
        }
    }

    private void publishData(String data) throws IOException {
        InetAddress ip = InetAddress.getLocalHost();
        Socket socket = new Socket(ip, Constants.MANAGER_SERVER_PORT);
        DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
        dos.writeUTF(data);
        dos.close();
        socket.close();
    }
}