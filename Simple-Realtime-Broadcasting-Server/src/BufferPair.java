import java.nio.ByteBuffer;

public class BufferPair {
    private ByteBuffer inputBuffer;

    private ByteBuffer outputBuffer;

    private final int bufferSize = 1 << 10;

    public BufferPair() {
        this.inputBuffer = ByteBuffer.allocate(this.bufferSize);
        this.outputBuffer = ByteBuffer.allocate(this.bufferSize);
    }

    public ByteBuffer getInputBuffer() {
        return this.inputBuffer;
    }

    public ByteBuffer getOutputBuffer() {
        return this.outputBuffer;
    }
}
