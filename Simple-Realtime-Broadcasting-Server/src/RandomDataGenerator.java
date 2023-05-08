import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListSet;

public class RandomDataGenerator implements Runnable {

    /**
     * The generator will generate integers between [0, range)
     */
    private int range;

    /**
     * The random integer Generator
     */
    private Random randomIntegerGenerator;

    /**
     * Write generated random integers to "server_data.txt"
     */
    private FileWriter fileWriter;

    /**
     * The default file path of server data file
     */
    private String filePath = "server_data.txt";

    /**
     * The date formatter which converts the current time to a more precise one
     */
    SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    private ConcurrentSkipListSet<SocketChannel> historyDataSent;

    private ConcurrentSkipListSet<SocketChannel> newHistoryDataSent;

    private HashMap<SocketChannel, BufferPair> socketChannel2BufferPair;

    private HashMap<SocketChannel, Date> lastHeartBeatTime;

    public RandomDataGenerator(int range, ConcurrentSkipListSet<SocketChannel> historyDataSent, ConcurrentSkipListSet<SocketChannel> newHistoryDataSent, HashMap<SocketChannel, BufferPair> socketChannel2BufferPair, HashMap<SocketChannel, Date> lastHeartBeatTime) {
        this.range = range;
        this.randomIntegerGenerator = new Random();
        while (true) {
            try {
                this.fileWriter = new FileWriter(this.filePath, true);
                break;
            } catch (IOException e) {
                e.printStackTrace();
                System.out.println("Please assign another server data file to the server. Input the path below: ");
                Scanner scanner = new Scanner(System.in);
                filePath = scanner.nextLine();
            }
        }
        this.historyDataSent = historyDataSent;
        this.newHistoryDataSent = newHistoryDataSent;
        this.socketChannel2BufferPair = socketChannel2BufferPair;
        this.lastHeartBeatTime = lastHeartBeatTime;
    }

    private void closeASocketChannel(SocketChannel socketChannel) {
        if (socketChannel != null) {
            try {
                socketChannel.close();
            } catch (IOException e) {
                e.printStackTrace();
                System.err.println("Cannot close the given socket channel: " + e.getMessage());
                this.socketChannel2BufferPair.remove(socketChannel);
                this.lastHeartBeatTime.remove(socketChannel);
                this.historyDataSent.remove(socketChannel);
                this.newHistoryDataSent.remove(socketChannel);
                this.lastHeartBeatTime.remove(socketChannel);
            }
        }
    }

    @Override
    public void run() {
        int randomInteger;
        while (true) {
            // generate the random int
            randomInteger = this.randomIntegerGenerator.nextInt(this.range);
            // prepare data
            String formattedTime = this.dateFormatter.format(new Date(System.currentTimeMillis()));
            String line = "[" + formattedTime + "]" + "::" + randomInteger;
            // save to the file
            try {
                fileWriter.write(line + "\n");
                fileWriter.flush();
            } catch (IOException e) {
                e.printStackTrace();
                System.out.println("An error occurred when attempting to write to the file \"" + filePath + "\"\n Retrying with the same path...");
                while (true) {
                    try {
                        this.fileWriter = new FileWriter(this.filePath, true);
                        break;
                    } catch (IOException ee) {
                        ee.printStackTrace();
                        System.out.println("Retry failed. Please assign another server data file to the server. Input the path below: ");
                        Scanner scanner = new Scanner(System.in);
                        filePath = scanner.nextLine();
                    }
                }
            }
            // update `historyDataSent` with `newHistoryDataSent`
            this.historyDataSent.clear();
            this.historyDataSent.addAll(this.newHistoryDataSent);
            // send real-time data
            Iterator<SocketChannel> iterator = this.historyDataSent.iterator();
            while (iterator.hasNext()) {
                // get the socket channel
                SocketChannel socketChannel = iterator.next();
                // get the output buffer
                if (!socketChannel2BufferPair.containsKey(socketChannel)) {
                    socketChannel2BufferPair.put(socketChannel, new BufferPair());
                }
                ByteBuffer outputBuffer = socketChannel2BufferPair.get(socketChannel).getOutputBuffer();
                if (outputBuffer == null) {
                    System.err.println("Failed to allocate output buffer for the current socket channel. Please check the memory usage or restart your server.");
                    // close the socket and free the resources
                    this.closeASocketChannel(socketChannel);
                }
                // prepare the msg header
                int commandID = CommandID.DATA_RESP;
                int totalLength = FieldLength.HEADER + FieldLength.TIMESTAMP + FieldLength.DATA;
                // prepare the msg body
                String timestamp = "[" + formattedTime + "]";
                // encapsulate the msg
                outputBuffer.putInt(commandID);
                outputBuffer.putInt(totalLength);
                outputBuffer.put(timestamp.getBytes(StandardCharsets.US_ASCII));
                outputBuffer.putInt(randomInteger);
                // switch to the read mode
                outputBuffer.flip();
                try {
                    // send the data
                    socketChannel.write(outputBuffer);
                    // switch to the write mode
                    outputBuffer.compact();
                } catch (IOException e) {
                    e.printStackTrace();
                    System.err.println("Cannot write data to the current socket.");
                    this.closeASocketChannel(socketChannel);
                }
            }
            // sleep 250ms
            try {
                Thread.sleep(250);
            } catch (InterruptedException e) {
                System.out.println("The data generation thread was interrupted while sleeping");
                // Re-interrupt the thread to preserve the interruption status
                Thread.currentThread().interrupt();
            }
        }
    }
}
