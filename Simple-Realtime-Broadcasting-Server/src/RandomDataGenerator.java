import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Date;
import java.util.Random;
import java.util.Scanner;

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
    private BufferedWriter bufferedWriter;

    /**
     * The default file path of server data file
     */
    private String filePath = "server_data.txt";

    public RandomDataGenerator(int range) {
        this.range = range;
        this.randomIntegerGenerator = new Random();
        while (true) {
            try {
                this.bufferedWriter = new BufferedWriter(new FileWriter(this.filePath, true));
                break;
            } catch (IOException e) {
                e.printStackTrace();
                System.out.println("Please assign another server data file to the server. Input the path below: ");
                Scanner scanner = new Scanner(System.in);
                filePath = scanner.nextLine();
            }
        }
    }

    @Override
    public void run() {
        int randomInteger;
        while (true) {
            // generate the random int
            randomInteger = this.randomIntegerGenerator.nextInt(this.range);
            // sleep 250ms
            try {
                Thread.sleep(250);
            } catch (InterruptedException e) {
                System.out.println("The data generation thread was interrupted while sleeping");
                // Re-interrupt the thread to preserve the interruption status
                Thread.currentThread().interrupt();
            }
            // prepare data
            String line = "[" + new Date() + "]" + "::" + randomInteger;
            // save to the file
            try {
                bufferedWriter.write(line);
            } catch (IOException e) {
                e.printStackTrace();
                System.out.println("An error occurred when attempting to write to the file \"" + filePath + "\"\n Retrying with the same path...");
                while (true) {
                    try {
                        this.bufferedWriter = new BufferedWriter(new FileWriter(this.filePath, true));
                        break;
                    } catch (IOException ee) {
                        ee.printStackTrace();
                        System.out.println("Retry failed. Please assign another server data file to the server. Input the path below: ");
                        Scanner scanner = new Scanner(System.in);
                        filePath = scanner.nextLine();
                    }
                }
            }
        }
    }
}
