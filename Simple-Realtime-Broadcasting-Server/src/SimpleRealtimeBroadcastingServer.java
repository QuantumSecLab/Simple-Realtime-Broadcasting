import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.*;
import java.util.Iterator;
import java.util.Scanner;
import java.util.Set;

public class SimpleRealtimeBroadcastingServer {

    private ServerSocketChannel serverSocketChannel;

    private Selector selector;

    private int portNumber = 11451;

    public SimpleRealtimeBroadcastingServer() {
        // open socket channel
        try {
            this.serverSocketChannel = ServerSocketChannel.open();
        } catch (IOException e) {
            e.printStackTrace();
            System.err.println("Failed to open the server socket channel: " + e.getMessage());
            System.exit(1);
        }
        // bind socket to a port
        while (true) {
            try {
                this.serverSocketChannel.socket().bind(new InetSocketAddress(this.portNumber));
                break;
            } catch (IOException e) {
                e.printStackTrace();
                System.err.println("Invalid port number. Please assign another port number: ");
                Scanner scanner = new Scanner(System.in);
                this.portNumber = scanner.nextInt();
            }
        }
        // open a selector
        try {
            this.selector = Selector.open();
        } catch (IOException e) {
            e.printStackTrace();
            System.err.println("Failed to open the selector: " + e.getMessage());
            if (serverSocketChannel != null) {
                try {
                    serverSocketChannel.close();
                } catch (IOException ee) {
                    ee.printStackTrace();
                    System.err.println("Failed to close the server socket channel: " + ee.getMessage());
                }
            }
            System.exit(1);
        }
        // register the server socket channel to the selector
        try {
            serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
        } catch (ClosedChannelException e) {
            e.printStackTrace();
            System.err.println("The socket channel was closed unexpectedly: " + e.getMessage());
            if (selector != null) {
                try {
                    selector.close();
                } catch (IOException ee) {
                    ee.printStackTrace();
                    System.err.println("Failed to close the selector: " + ee.getMessage());
                }
            }
            System.exit(1);
        }
    }

    public void launch() {
        // start data generator thread
        Thread dataGeneratorThread = new Thread(new RandomDataGenerator(100));
        dataGeneratorThread.setDaemon(true);
        dataGeneratorThread.start();
        // start heart beat monitor thread

        // start pushing data
        while (true) {
            // select the socket which has the new event
            try {
                this.selector.select();
            } catch (IOException e) {
                e.printStackTrace();
                System.err.println("Cannot call select() method on the selector: " + e.getMessage());
                System.exit(1);
            } catch (ClosedSelectorException e) {
                e.printStackTrace();
                System.err.println("The selector was closed unexpectedly: " + e.getMessage());
                System.exit(1);
            }
            // get selection keys
            Set<SelectionKey> keys = this.selector.selectedKeys();
            Iterator<SelectionKey> iterator = keys.iterator();
            // iterate through the keys and process the tasks
            while (iterator.hasNext()) {
                SelectionKey key = iterator.next();
                iterator.remove();
                if (key.isAcceptable()) {
                    accept(key);
                }
            }
        }
    }

    private void accept(SelectionKey key) {
        ServerSocketChannel serverSocket = (ServerSocketChannel) key.channel();
        try {
            SocketChannel socketChannel = serverSocket.accept();
        } catch (IOException e) {
            e.printStackTrace();
            System.err.println("Failed to accept the new connection: " + e.getMessage());
            System.exit(1);
        }
    }

    public static void main(String args[]) {
        SimpleRealtimeBroadcastingServer server = new SimpleRealtimeBroadcastingServer();
        server.launch();
    }
}
