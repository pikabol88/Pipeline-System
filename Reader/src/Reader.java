import ru.spbstu.pipeline.*;
import ru.spbstu.pipeline.RC;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Reader implements IReader {

    enum READER_GRAMMAR {
        BUFFER_SIZE
    }
    public static final String GRAMMAR_SEPARATOR = "=";

    INotifier notifier;
    public Configer config;
    private Logger LOGGER;

    private InputStream inputStream;
    private int bufferSize;
    private int currentChunk = 0;
    private final HashMap<Integer, byte[]> chunks = new HashMap<>();

    public Reader(Logger logger){
        LOGGER = logger;
    }

    @Override
    public void run() {
        RC state=RC.CODE_SUCCESS;
        state = execute();
        if (state!=RC.CODE_SUCCESS) {
            LOGGER.log(Level.SEVERE, ErrorDescription.getDescription(state));
        }
        LOGGER.log(Level.INFO, "reader throw processing is finished");
    }

    @Override
    public RC setInputStream(FileInputStream fis) {
        inputStream = fis;
        return RC.CODE_SUCCESS;
    }

    @Override
    public RC setConfig(String cfg) {
        config = new Configer(cfg, READER_GRAMMAR.values(),GRAMMAR_SEPARATOR, true, LOGGER);
        bufferSize = Integer.parseInt(config.config.get(READER_GRAMMAR.BUFFER_SIZE.toString()));
        return config.errorState;
    }

    @Override
    public RC addNotifier(INotifier iNotifier) {
        notifier = iNotifier;
        return RC.CODE_SUCCESS;
    }


    public RC execute() {
        LOGGER.log(Level.INFO, "start reader execution");
        byte[] buffer;
        do {
            try {
                buffer = binaryReader();
                chunks.put(currentChunk, buffer);
                notifier.notify(currentChunk);
               // System.out.println("read "+currentChunk +" "+ Arrays.toString(buffer));
                currentChunk++;
            } catch (IOException e) {
                LOGGER.log(Level.SEVERE, "failed to read from the input stream");
                return RC.CODE_FAILED_TO_READ;
            }
        } while (buffer != null);
        LOGGER.log(Level.INFO, "end reader execution");
        return RC.CODE_SUCCESS;
    }


    public byte[] binaryReader() throws IOException {

        byte[] buffer = new byte[bufferSize];
        int currentSize = 0;
        while((currentSize = inputStream.read(buffer) )!= -1) {
            if(currentSize != bufferSize){
                return deleteZero(buffer, currentSize);
            }
            return buffer;
        }
        return null;
    }

    private byte[] deleteZero(byte[] bytes, int size){
        byte[] new_bytes = new byte[size];
        if (size >= 0) System.arraycopy(bytes, 0, new_bytes, 0, size);
        return new_bytes;
    }

    @Override
    public TYPE[] getOutputTypes() {
        return new TYPE[]{TYPE.SHORT,TYPE.CHAR, TYPE.BYTE};
    }


    @Override
    public IMediator getMediator(TYPE type) {
        IMediator obj;
        switch (type){
            case BYTE : obj = new Reader.ByteMediator(); break;
            case CHAR : obj = new Reader.CharMediator(); break;
            case SHORT : obj = new Reader.ShortMediator(); break;
            default : throw new IllegalStateException("Unexpected value: " + type);
        }
        return obj;
    }

    private class ByteMediator implements IMediator {
        @Override
        public Object getData(int i) {
            if(chunks.size()>0) {
                byte[] partOfOutput = chunks.get(i);
                chunks.remove(i);
                if (partOfOutput == null) {
                    return null;
                }
                byte[] copy = new byte[partOfOutput.length];
                System.arraycopy(partOfOutput, 0, copy, 0, partOfOutput.length);
                return copy;
            }
            return null;
        }
    }

    private class ShortMediator implements IMediator {
        @Override
        public Object getData(int i) {
            if(chunks.size()>0) {
                byte[] partOfOutput = chunks.get(i);
                chunks.remove(i);
                if (partOfOutput == null) return null;
                int size = partOfOutput.length;
                ByteBuffer buffer = ByteBuffer.wrap(partOfOutput);
                short[] shortArray = new short[size / 2];
                for (int index = 0; index < size / 2; ++index) {
                    shortArray[index] = buffer.getShort(2 * index);
                }
                if (shortArray.length > 0) {
                    return shortArray;
                }
            }
            return null;
        }
    }

    private class CharMediator implements IMediator {
        @Override
        public Object getData(int i) {
            if (chunks.size() > 0) {
                byte[] partOfOutput = chunks.get(i);
                chunks.remove(i);
                if (partOfOutput == null) return null;
                return new String(partOfOutput, StandardCharsets.UTF_8).toCharArray();
            }
            return null;
        }
    }
}
