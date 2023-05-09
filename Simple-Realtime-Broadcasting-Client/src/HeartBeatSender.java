import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Date;

public class HeartBeatSender implements Runnable {

    private SocketChannel socketChannel;

    public HeartBeatSender(SocketChannel socketChannel) {
        this.socketChannel = socketChannel;
    }

    @Override
    public void run() {
        while (true) {
            ByteBuffer outputBuffer = ByteBuffer.allocate(1 << 10);
            outputBuffer.putInt(FieldLength.HEADER);
            outputBuffer.putInt(CommandID.HEART_BEAT);
            outputBuffer.flip();
            try {
                this.socketChannel.write(outputBuffer);
                outputBuffer.compact();
                System.out.println("[" + new Date() + "] " + "A heartbeat was sent.");
            } catch (IOException e) {
                e.printStackTrace();
                System.err.println("The client socket channel cannot sent heartbeat: " +e.getMessage());
                try {
                    socketChannel.close();
                } catch (IOException ex) {
                    ex.printStackTrace();
                    System.err.println("The client socket channel cannot be closed: " +ex.getMessage());
                }
                System.exit(1);
            }
            try {
                Thread.sleep(30000);
            } catch (InterruptedException e) {
                e.printStackTrace();
                System.err.println("The heartbeat sender thread was interrupted when sleeping: " + e.getMessage());
                // re-interrupt
                Thread.currentThread().interrupt();
            }
        }
    }
}
