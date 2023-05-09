import java.io.*;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CopyOnWriteArraySet;

public class SimpleRealtimeBroadcastingServer {

    private ServerSocketChannel serverSocketChannel;

    private Selector selector;

    private int portNumber = 11451;

    private ConcurrentHashMap<SocketChannel, BufferPair> socketChannel2BufferPair;

    private ConcurrentHashMap<SocketChannel, Date> lastHeartBeatTime;

    private String filePath = "server_data.txt";

    private CopyOnWriteArraySet<SocketChannel> historyDataSent;

    private CopyOnWriteArraySet<SocketChannel> newHistoryDataSent;

    private void closeAllSocketChannels() {
        Set<SocketChannel> socketChannelSet = socketChannel2BufferPair.keySet();
        Iterator<SocketChannel> iterator = socketChannelSet.iterator();
        while (iterator.hasNext()) {
            SocketChannel socketChannel = iterator.next();
            this.closeASocketChannel(socketChannel);
        }
    }

    private void closeTheServerSocketChannel() {
        if (this.serverSocketChannel != null) {
            try {
                this.serverSocketChannel.close();
            } catch (IOException e) {
                e.printStackTrace();
                System.err.println("Failed to close the server socket channel: " + e.getMessage());
            }
        }
    }

    private void closeTheSelector() {
        if (this.selector != null) {
            try {
                this.selector.close();
            } catch (IOException e) {
                e.printStackTrace();
                System.err.println("Failed to close the selector: " + e.getMessage());
            }
        }
    }

    private void closeASocketChannel(SocketChannel socketChannel) {
        if (socketChannel != null) {
            try {
                socketChannel.close();
                this.socketChannel2BufferPair.remove(socketChannel);
                this.lastHeartBeatTime.remove(socketChannel);
                this.historyDataSent.remove(socketChannel);
                this.newHistoryDataSent.remove(socketChannel);
                this.lastHeartBeatTime.remove(socketChannel);
            } catch (IOException e) {
                e.printStackTrace();
                System.err.println("Cannot close the given socket channel: " + e.getMessage());
            }
        }
    }

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
        // set the server socket channel to the non-blocking mode
        try {
            this.serverSocketChannel.configureBlocking(false);
        } catch (IOException e) {
            e.printStackTrace();
            System.err.println("Failed to set the server socket channel to the non-blocking mode: " + e.getMessage());
            this.closeTheServerSocketChannel();
            System.exit(1);
        }
        // open a selector
        try {
            this.selector = Selector.open();
        } catch (IOException e) {
            e.printStackTrace();
            System.err.println("Failed to open the selector: " + e.getMessage());
            this.closeTheServerSocketChannel();
            System.exit(1);
        }
        // register the server socket channel to the selector
        try {
            serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
        } catch (ClosedChannelException e) {
            e.printStackTrace();
            System.err.println("The socket channel was closed unexpectedly: " + e.getMessage());
            this.closeTheSelector();
            System.exit(1);
        }
        // initialize the `socketChannel2BufferPair` and `lastHeartBeatTime` ConcurrentHashMap
        this.lastHeartBeatTime = new ConcurrentHashMap<>();
        this.socketChannel2BufferPair = new ConcurrentHashMap<>();
        // initialize `historyDataSent` and `newHistoryDataSent` sets.
        this.historyDataSent = new CopyOnWriteArraySet<>();
        this.newHistoryDataSent = new CopyOnWriteArraySet<>();

        System.out.println("The server started successfully on the port " + this.portNumber);
    }

    public void launch() {
        // start data generator thread
        Thread dataGeneratorThread = new Thread(new RandomDataGenerator(100, this.historyDataSent, this.newHistoryDataSent, this.socketChannel2BufferPair, this.lastHeartBeatTime));
        dataGeneratorThread.setDaemon(true);
        dataGeneratorThread.start();
        // start heart beat monitor thread
        Thread heartbeatMonitor = new Thread(new HeartBeatMonitor(this.lastHeartBeatTime, this.historyDataSent, this.newHistoryDataSent, this.socketChannel2BufferPair));
        heartbeatMonitor.setDaemon(true);
        heartbeatMonitor.start();
        // start pushing the history data
        while (true) {
            // select the socket which has the new event
            try {
                this.selector.select();
            } catch (IOException e) {
                e.printStackTrace();
                System.err.println("Cannot call select() method on the selector: " + e.getMessage());
                this.closeTheServerSocketChannel();
                this.closeTheSelector();
                System.exit(1);
            } catch (ClosedSelectorException e) {
                e.printStackTrace();
                System.err.println("The selector was closed unexpectedly: " + e.getMessage());
                this.closeTheServerSocketChannel();
                System.exit(1);
            }
            // get selection keys
            Set<SelectionKey> keys = this.selector.selectedKeys();
            Iterator<SelectionKey> iterator = keys.iterator();
            // iterate through the keys and process the tasks
            while (iterator.hasNext()) {
                SelectionKey key = iterator.next();
                iterator.remove();
                if (!key.isValid()) {
                    continue;
                }
                if (key.isAcceptable()) {
                    this.accept(key);
                } else if (key.isReadable()) {
                    // perform read operation on the given socket
                    if (this.read(key) == StatusCode.FAIL) {
                        // close the socket if fail
                        this.closeASocketChannel((SocketChannel) key.channel());
                    }
                    if (this.process(key) == StatusCode.FAIL) {
                        // close the socket if fail
                        this.closeASocketChannel((SocketChannel) key.channel());
                    }
                }
            }
        }
    }

    private int sendHistoryData(SocketChannel socketChannel) {
        // open the file in read mode
        BufferedReader reader = null;
        while (true) {
            try {
                reader = new BufferedReader(new FileReader(this.filePath));
                break;
            } catch (FileNotFoundException e) {
                e.printStackTrace();
                System.err.println("Cannot find the server data file in the given path. Please locate the data file manually: ");
                Scanner scanner = new Scanner(System.in);
                this.filePath = scanner.nextLine();
            }
        }
        // send the history data to the client
        try {
            String currentLine = reader.readLine();
            for (int currentLineNumber = 0; currentLine != null; currentLineNumber ++) {
                // prepare the msg header data
                int serverTotalLength = FieldLength.HEADER + FieldLength.TIMESTAMP + FieldLength.DATA;
                int serverCommandID = CommandID.DATA_RESP;
                // prepare the msg body data
                String[] parts = currentLine.split("::");
                String serverTimestamp = parts[0];
                int serverData = Integer.parseInt(parts[1]);
                // get the output buffer
                if (!socketChannel2BufferPair.containsKey(socketChannel)) {
                    socketChannel2BufferPair.put(socketChannel, new BufferPair());
                }
                ByteBuffer outputBuffer = socketChannel2BufferPair.get(socketChannel).getOutputBuffer();
                if (outputBuffer == null) {
                    System.err.println("Failed to allocate output buffer for the current socket channel. Please check the memory usage or restart your server.");
                    reader.close();
                    return StatusCode.FAIL;
                }
                // encapsulate the msg
                outputBuffer.putInt(serverTotalLength);
                outputBuffer.putInt(serverCommandID);
                outputBuffer.put(serverTimestamp.getBytes(StandardCharsets.US_ASCII));
                outputBuffer.putInt(serverData);
                // switch to read mode
                outputBuffer.flip();
                // send the data
                try {
                    socketChannel.write(outputBuffer);
                } catch (IOException e) {
                    e.printStackTrace();
                    System.err.println("Failed to send the data on the current socket channel.");
                    this.closeASocketChannel(socketChannel);
                }
                // switch to the write mode
                outputBuffer.compact();
                // read a new line
                currentLine = reader.readLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.err.println("An error occurred when attempting to read a line from the file.");
            // close the BufferedReader
            try {
                reader.close();
            } catch (IOException ex) {
                ex.printStackTrace();
                System.err.println("Cannot close the current BufferedReader.");
            }

            return StatusCode.FAIL;
        }
        // add the socket to the `newHistoryDataSent` set
        this.newHistoryDataSent.add(socketChannel);
        // close the BufferedReader
        try {
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
            System.err.println("Cannot close the current BufferedReader.");
        }

        return StatusCode.SUCCESS;
    }

    private int process(SelectionKey key) {
        // get the socket channel
        SocketChannel socketChannel = (SocketChannel) key.channel();
        // get the input buffer
        // error detection
        if (!this.socketChannel2BufferPair.containsKey(socketChannel))
        {
            // should never happen
            System.err.println("The given socket channel does NOT have a corresponding BufferPair.");
            this.socketChannel2BufferPair.put(socketChannel, new BufferPair());
            return StatusCode.FAIL;
        }
        ByteBuffer inputBuffer;
        // error detection
        if ((inputBuffer = this.socketChannel2BufferPair.get(socketChannel).getInputBuffer()) == null ) {
            // should never happen
            System.err.println("The given socket cannot be allocated a input buffer. Check your memory usage or restart the server.");
            return StatusCode.FAIL;
        }

        // test whether the msg header is complete
        if (inputBuffer.remaining() < FieldLength.HEADER) {
            // switch back to the write mode
            inputBuffer.compact();

            return StatusCode.NOT_COMPLETE;
        }
        // parse the header
        int clientTotalLength = inputBuffer.getInt();
        int clientCommandID = inputBuffer.getInt();
        // test whether the msg body is complete
        int bodyLength = clientTotalLength - FieldLength.HEADER;
        if (inputBuffer.remaining() < bodyLength) {
            // put the msg header back to the `inputBuffer`
            inputBuffer.position(inputBuffer.position() - FieldLength.HEADER);
            // switch back to the read mode
            inputBuffer.compact();

            return StatusCode.NOT_COMPLETE;
        }
        // read the msg body into a byte array
        byte[] body = new byte[bodyLength];
        inputBuffer.get(body, 0, bodyLength);
        // switch to the write mode
        inputBuffer.compact();
        // select the corresponding operation by `commandID`
        switch (clientCommandID) {
            case CommandID.DATA_REQ: {
                // parse the msg body
                // parse the timestamp field
                byte[] timestampBytes = new byte[FieldLength.TIMESTAMP];
                System.arraycopy(body, 0, timestampBytes, 0, FieldLength.TIMESTAMP);
                String lastTimestamp = new String(timestampBytes, StandardCharsets.US_ASCII);
                // parse the data field
                byte[] dataBytes = new byte[FieldLength.DATA];
                System.arraycopy(body, FieldLength.TIMESTAMP, dataBytes, 0, FieldLength.DATA);
                int lastData = ByteBuffer.wrap(dataBytes).getInt();
                // send history data
                if(this.sendHistoryData(socketChannel) == StatusCode.FAIL)
                    return StatusCode.FAIL;

                break;
            }
            case CommandID.HEART_BEAT: {
                this.updateTheLastHeartBeatTime(socketChannel);
                try {
                    System.out.println("[" + new Date() + "] " + "A heartbeat was received from " + socketChannel.getRemoteAddress());
                } catch (IOException e) {
                    e.printStackTrace();
                    System.err.println("[" + new Date() + "] " + "A heartbeat was received but we cannot get the remote address.");
                }
                break;
            }
            default: {
                // should never happen
                return StatusCode.FAIL;
            }
        }

        return StatusCode.SUCCESS;
    }

    private void updateTheLastHeartBeatTime(SocketChannel socketChannel) {
        this.lastHeartBeatTime.put(socketChannel, new Date());
    }

    private int matchHistoryData(String clientTimestamp, int clientData) {
        // open the file with FileReader until succeeded
        BufferedReader reader;
        while (true) {
            try {
                reader = new BufferedReader(new FileReader(this.filePath));
                break;
            } catch (IOException e) {
                e.printStackTrace();
                System.err.println("Cannot find the server data file in the given path. Please locate the data file manually: ");
                Scanner scanner = new Scanner(System.in);
                this.filePath = scanner.nextLine();
            }
        }
        // iterate through the entire file line by line
        try {
            String line = reader.readLine();
            int lineNumber = 0;
            while (line != null) {
                String[] parts = line.split("::");
                String serverTimestamp = parts[0];
                String serverDataString = parts[1];
                int serverData = Integer.parseInt(serverDataString);
                if (clientTimestamp.equals(serverTimestamp) && clientData == serverData)
                {
                    // close the BufferedReader
                    try {
                        reader.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                        System.err.println("Cannot close the current BufferedReader.");
                    }
                    return lineNumber;
                }
                else lineNumber ++;
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("An error occurred when attempting to read a line from the data file.");
            // close the BufferedReader
            try {
                reader.close();
            } catch (IOException ex) {
                ex.printStackTrace();
                System.err.println("Cannot close the current BufferedReader.");
            }

            return StatusCode.FAIL;
        }
        // close the BufferedReader
        try {
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
            System.err.println("Cannot close the current BufferedReader.");
        }

        return StatusCode.FAIL;
    }

    private int read(SelectionKey key) {
        // get the socket channel from the selection key
        SocketChannel socketChannel = (SocketChannel) key.channel();
        // error detection
        if (!this.socketChannel2BufferPair.containsKey(socketChannel))
        {
            // should never happen
            System.err.println("The given socket channel does NOT have a corresponding BufferPair.");
            this.socketChannel2BufferPair.put(socketChannel, new BufferPair());
        }
        ByteBuffer inputBuffer;
        // error detection
        if ((inputBuffer = this.socketChannel2BufferPair.get(socketChannel).getInputBuffer()) == null ) {
            // should never happen
            System.err.println("The given socket cannot be allocated a input buffer. Check your memory usage or restart the server.");
            return StatusCode.FAIL;
        }
        // bulk read
        try {
            socketChannel.read(inputBuffer);
        } catch (IOException e) {
            e.printStackTrace();
            System.err.println("Cannot read the input buffer of the given socket channel.");
        }
        // switch the input buffer to the read mode
        inputBuffer.flip();

        return StatusCode.SUCCESS;
    }

    private ByteBuffer createAByteBuffer() {
        int bufferSize = 1 << 10;
        return ByteBuffer.allocate(bufferSize);
    }

    private void accept(SelectionKey key) {
        ServerSocketChannel serverSocket = (ServerSocketChannel) key.channel();
        SocketChannel socketChannel = null;
        // accept the new connection
        try {
            socketChannel = serverSocket.accept();
        } catch (IOException e) {
            e.printStackTrace();
            System.err.println("Failed to accept the new connection: " + e.getMessage());
            return;
        }
        // configure the new socket channel to the non-blocking mode
        try {
            socketChannel.configureBlocking(false);
        } catch (IOException e) {
            e.printStackTrace();
            System.err.println("Cannot set the socket channel to the non-blocking mode: " + e.getMessage());
            this.closeASocketChannel(socketChannel);
            return;
        }
        // register the socket channel to the server selector
        try {
            socketChannel.register(this.selector, SelectionKey.OP_READ | SelectionKey.OP_WRITE);
        } catch (ClosedChannelException e) {
            e.printStackTrace();
            System.err.println("Cannot register the given socket channel to the server selector since the socket channel has been closed unexpectedly: " +e.getMessage());
        }
        // add the new socket channel to the hash map
        this.socketChannel2BufferPair.put(socketChannel, new BufferPair());
        // add the new socket channel to the `lastHeartBeatTime` hash map
        this.lastHeartBeatTime.put(socketChannel, new Date());
    }

    public static void main(String args[]) {
        SimpleRealtimeBroadcastingServer server = new SimpleRealtimeBroadcastingServer();
        server.launch();
    }
}
