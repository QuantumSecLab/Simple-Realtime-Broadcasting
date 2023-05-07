public class SimpleRealtimeBroadcastingServer {

    public static void main(String args[]) {
        Thread dataGeneratorThread = new Thread(new RandomDataGenerator(100));
        dataGeneratorThread.setDaemon(true);
        dataGeneratorThread.start();
        
    }
}
