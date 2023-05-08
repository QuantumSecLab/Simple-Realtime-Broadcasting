import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;

public class HeartBeatMonitor implements Runnable {

    private HashMap<SocketChannel, Date> lastHeartBeatTime;

    private ConcurrentSkipListSet<SocketChannel> historyDataSent;

    private ConcurrentSkipListSet<SocketChannel> newHistoryDataSent;

    private HashMap<SocketChannel, BufferPair> socketChannel2BufferPair;

    public HeartBeatMonitor(HashMap<SocketChannel, Date> lastHeartBeatTime, ConcurrentSkipListSet<SocketChannel> historyDataSent, ConcurrentSkipListSet<SocketChannel> newHistoryDataSent, HashMap<SocketChannel, BufferPair> socketChannel2BufferPair) {
        this.lastHeartBeatTime = lastHeartBeatTime;
        this.historyDataSent = historyDataSent;
        this.newHistoryDataSent = newHistoryDataSent;
        this.socketChannel2BufferPair = socketChannel2BufferPair;
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
        while (true) {
            Set<SocketChannel> socketChannelSet = this.lastHeartBeatTime.keySet();
            Iterator<SocketChannel> iterator = socketChannelSet.iterator();
            while (iterator.hasNext()) {
                SocketChannel socketChannel = iterator.next();
                Date lastHeartBeat = this.lastHeartBeatTime.get(socketChannel);
                long timeDiff = (new Date()).getTime() - lastHeartBeat.getTime();
                if (timeDiff > 120000) {
                    System.out.println(socketChannel + "heartbeat timed out.");
                    this.closeASocketChannel(socketChannel);
                }
            }
            // check the client for every 60 seconds
            try {
                Thread.sleep(60000);
            } catch (InterruptedException e) {
                System.out.println("The heart beat monitor thread was interrupted while sleeping");
                // Re-interrupt the thread to preserve the interruption status
                Thread.currentThread().interrupt();
            }
        }
    }
}
