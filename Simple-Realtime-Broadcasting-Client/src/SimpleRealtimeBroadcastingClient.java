import java.io.*;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Scanner;
import java.util.Set;

public class SimpleRealtimeBroadcastingClient {

    private SocketChannel socketChannel;

    private ByteBuffer inputBuffer;

    private ByteBuffer outputBuffer;

    private int bufferSize = 1 << 10;

    private Selector selector;

    private String filePath = "client_data.txt";

    private FileWriter fileWriter;

    private void closeTheSocketChannel() {
        try {
            this.socketChannel.close();
        } catch (IOException e) {
            e.printStackTrace();
            System.err.println("Cannot close the client socket channel: " + e.getMessage());
            System.exit(1);
        }
    }

    private void closeTheSelector() {
        try {
            this.selector.close();
        } catch (IOException e) {
            e.printStackTrace();
            System.err.println("Cannot close the selector: " + e.getMessage());
            System.exit(1);
        }
    }

    public SimpleRealtimeBroadcastingClient(String remoteIP, int remotePort) {
        try {
            this.socketChannel = SocketChannel.open(new InetSocketAddress(remoteIP, remotePort));
        } catch (IOException e) {
            e.printStackTrace();
            System.err.println("Cannot open the socket channel.");
            System.exit(1);
        }
        try {
            this.socketChannel.configureBlocking(false);
        } catch (IOException e) {
            e.printStackTrace();
            System.err.println("Cannot set the socket channel to the non-blocking mode: " + e.getMessage());
            this.closeTheSocketChannel();
            System.exit(1);
        }
        try {
            this.selector = Selector.open();
        } catch (IOException e) {
            e.printStackTrace();
            System.err.println("Cannot open the selector: " + e.getMessage());
            this.closeTheSocketChannel();
            System.exit(1);
        }
        try {
            this.socketChannel.register(selector, SelectionKey.OP_READ | SelectionKey.OP_WRITE | SelectionKey.OP_CONNECT);
        } catch (ClosedChannelException e) {
            e.printStackTrace();
            System.err.println("Cannot open the selector: " + e.getMessage());
            this.closeTheSelector();
            this.closeTheSocketChannel();
            System.exit(1);
        }
        this.inputBuffer = ByteBuffer.allocate(this.bufferSize);
        this.outputBuffer = ByteBuffer.allocate(this.bufferSize);
        while (true) {
            try {
                this.fileWriter = new FileWriter(this.filePath, true);
                break;
            } catch (IOException e) {
                e.printStackTrace();
                System.err.println("Cannot find the file. Please assign the location manually: " +e.getMessage());
                Scanner scanner = new Scanner(System.in);
                this.filePath = scanner.nextLine();
            }
        }
    }

    void sendDataReq() {
        // sending data req confirmation
        Scanner scanner = new Scanner(System.in);
        while (true) {
            System.out.println("Do you want to send a data request right now? (y/n)");
            if ((scanner.nextLine()).equals("y"))
                break;
        }
        // find the last record
        BufferedReader reader = null;
        while (true) {
            try {
                reader = new BufferedReader(new FileReader(this.filePath));
                break;
            } catch (FileNotFoundException e) {
                e.printStackTrace();
                System.err.println("Cannot find the client data file. Please assign the location manually: " + e.getMessage());
                this.filePath = scanner.nextLine();
            }
        }
        String currentLine = null;
        String previousLine = null;
        try {
            currentLine = reader.readLine();
            while (currentLine != null) {
                previousLine = currentLine;
                currentLine = reader.readLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.err.println("Cannot read a line from the client data file: " +e.getMessage());
            this.closeTheSelector();
            this.closeTheSocketChannel();
            System.exit(1);
        }
        // parse the last record
        if (previousLine != null) {
            // prepare the msg body data
            String[] parts = previousLine.split("::");
            String timestamp = parts[0];
            int data = Integer.parseInt(parts[1]);
            // encapsulate the msg
            this.outputBuffer.putInt(CommandID.DATA_REQ);
            this.outputBuffer.putInt(FieldLength.HEADER + FieldLength.TIMESTAMP + FieldLength.DATA);
            this.outputBuffer.put(timestamp.getBytes(StandardCharsets.US_ASCII));
            this.outputBuffer.putInt(data);
            // switch to the read mode
            this.outputBuffer.flip();
        } else {
            byte[] timestamp = new byte[FieldLength.TIMESTAMP];
            Arrays.fill(timestamp, 0, FieldLength.TIMESTAMP, (byte) 0);
            byte[] data = new byte[FieldLength.DATA];
            Arrays.fill(data, 0, FieldLength.DATA, (byte) 0);
            // encapsulate the msg
            this.outputBuffer.putInt(CommandID.DATA_REQ);
            this.outputBuffer.putInt(FieldLength.HEADER + FieldLength.TIMESTAMP + FieldLength.DATA);
            this.outputBuffer.put(timestamp);
            this.outputBuffer.put(data);
            // switch to the read mode
            this.outputBuffer.flip();
        }
        // send the req
        try {
            socketChannel.write(outputBuffer);
            // switch to the write mode
            this.outputBuffer.compact();
        } catch (IOException e) {
            e.printStackTrace();
            System.err.println("Cannot send data on the client socket channel: " + e.getMessage());
            this.closeTheSelector();
            this.closeTheSocketChannel();
        }
    }

    public void launch() {
        // create a heartbeat sender thread
        Thread heartbeatSender = new Thread(new HeartBeatSender(this.socketChannel));
        heartbeatSender.setDaemon(true);
        heartbeatSender.start();
        // send the data req confirmation
        this.sendDataReq();
        // process received data
        while (true) {
            try {
                selector.select();
            } catch (IOException e) {
                e.printStackTrace();
                System.err.println("Cannot perform select() operation on the selector: " + e.getMessage());
                this.closeTheSelector();
                this.closeTheSocketChannel();
                System.exit(1);
            }
            Set<SelectionKey> keys = selector.selectedKeys();
            Iterator<SelectionKey> iterator = keys.iterator();
            while (iterator.hasNext()) {
                SelectionKey key = iterator.next();
                if (key.isReadable()) {
                    this.process(key);
                }
            }
        }
    }

    private void process(SelectionKey key) {
        SocketChannel socket = (SocketChannel) key.channel();
        // perform bulk read
        try {
            socket.read(inputBuffer);
            // switch to the read mode
            inputBuffer.flip();
        } catch (IOException e) {
            e.printStackTrace();
            System.err.println("Cannot read from the client socket channel: " +e.getMessage());
            this.closeTheSocketChannel();
            this.closeTheSelector();
            System.exit(1);
        }

        // process all tasks in one go
        while (true) {
            // test whether header is complete
            if (inputBuffer.remaining() < FieldLength.HEADER) {
                // switch to the write mode
                inputBuffer.compact();
                return;
            }
            // parse the header
            int commandID = inputBuffer.getInt();
            int totalLength = inputBuffer.getInt();
            // test whether the msg body is complete
            if (inputBuffer.remaining() < totalLength) {
                // put the header back to the buffer
                inputBuffer.position(inputBuffer.position() - FieldLength.HEADER);
                // switch to the write mode
                inputBuffer.compact();
                return;
            }
            // read the body
            byte[] body = new byte[totalLength - FieldLength.HEADER];
            inputBuffer.get(body);
            // switch to the write mode
            // select the operation by command ID
            switch (commandID) {
                case CommandID.DATA_RESP: {
                    // parse the timestamp
                    byte[] timestampBytes = new byte[FieldLength.TIMESTAMP];
                    System.arraycopy(body, 0, timestampBytes, 0, FieldLength.TIMESTAMP);
                    String timestamp = new String(timestampBytes, StandardCharsets.US_ASCII);
                    // parse the data
                    byte[] dataBytes = new byte[FieldLength.DATA];
                    System.arraycopy(body, FieldLength.TIMESTAMP, dataBytes, 0, FieldLength.DATA);
                    int data = ByteBuffer.wrap(dataBytes).getInt();
                    // write to the file
                    while (true) {
                        try {
                            this.fileWriter.write(timestamp + "::" + data);
                            this.fileWriter.flush();
                            break;
                        } catch (IOException e) {
                            e.printStackTrace();
                            System.err.println("Cannot write to the file. Please assign the location manually: " +e.getMessage());
                            Scanner scanner = new Scanner(System.in);
                            this.filePath = scanner.nextLine();
                        }
                    }
                    break;
                }
                default: {
                    // should never happen
                    break;
                }
            }
        }
    }

    public static void main(String args[]) {
        Scanner scanner = new Scanner(System.in);
        System.out.println("Please input the remote IP: ");
        String remoteIP = scanner.nextLine();
        System.out.println("Please input the remote port: ");
        int remotePort = scanner.nextInt();
        SimpleRealtimeBroadcastingClient client = new SimpleRealtimeBroadcastingClient(remoteIP, remotePort);
        client.launch();
    }
}
