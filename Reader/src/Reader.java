import ru.spbstu.pipeline.*;
import ru.spbstu.pipeline.RC;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Reader implements IReader {

    enum READER_GRAMMAR {
        BUFFER_SIZE
    }
    public static final String GRAMMAR_SEPARATOR = "=";

    private IConsumer consumer;

    public Configer config;
    private Logger LOGGER;

    private InputStream inputStream;
    private byte[] processed;
    private int bufferSize;

    public Reader(Logger logger){
        LOGGER = logger;
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
    public RC setConsumer(IConsumer iConsumer) {
        if(iConsumer==null) {
            LOGGER.log(Level.SEVERE, "invalid consumer object");
            return RC.CODE_INVALID_ARGUMENT;
        }
        consumer = iConsumer;
        return RC.CODE_SUCCESS;
    }

    @Override
    public RC setProducer(IProducer iProducer) {
        return RC.CODE_SUCCESS;
    }

    @Override
    public RC execute() {
        processed = new byte[bufferSize];
        byte[] buffer;
        do {
            try {
                buffer  = binaryReader();
            } catch (IOException e) {
                LOGGER.log(Level.SEVERE, "failed to read from the input stream");
                return RC.CODE_FAILED_TO_READ;
            }
            processed = buffer;
            consumer.execute();
        } while(buffer != null);
        return RC.CODE_SUCCESS;
    }

    public byte[] binaryReader() throws IOException {
        int count;
        byte[] buffer = new byte[bufferSize];
        while((count = inputStream.read(buffer) )!= -1) {
            if(count!= bufferSize){
                return  deleteZero(buffer, count);
            }
            return buffer;
        }
        return  null;
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
        public Object getData() {
            if(processed==null) return null;
            byte[] copy = new byte[processed.length];
            System.arraycopy(processed, 0, copy, 0, processed.length);
            return copy;
        }
    }

    private class ShortMediator implements IMediator {
        @Override
        public Object getData() {
            if(processed==null) return null;
            int size = processed.length;
            ByteBuffer buffer = ByteBuffer.wrap(processed);
            short[] shortArray = new short[size / 2];
            for(int index = 0; index < size / 2; ++index) {
                shortArray[index] = buffer.getShort(2 * index);
            }
            if(shortArray.length>0) {
                return shortArray;
            }
            return null;
        }
    }

    private class CharMediator implements IMediator {
        @Override
        public Object getData() {
            if(processed==null) return null;
            return new String(processed, StandardCharsets.UTF_8).toCharArray();
        }
    }
}
