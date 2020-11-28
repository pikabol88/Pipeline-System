import ru.spbstu.pipeline.IExecutable;
import ru.spbstu.pipeline.IReader;
import ru.spbstu.pipeline.RC;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Reader implements IReader {

    enum READER_GRAMMAR {
        BUFFER_SIZE
    }
    public static final String GRAMMAR_SEPARATOR = "=";

    Configer config;
    IExecutable consumer;
    InputStream inputStream;

    public int bufferSize;
    Logger LOGGER;

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
    public RC setConsumer(IExecutable c) {
        if(c==null) {
            LOGGER.log(Level.SEVERE, "invalid consumer object");
            return RC.CODE_INVALID_ARGUMENT;
        }
        consumer = c;
        return RC.CODE_SUCCESS;
    }

    @Override
    public RC setProducer(IExecutable p) {
        return RC.CODE_SUCCESS;
    }

    @Override
    public RC execute(byte[] data) {
        data = new byte[bufferSize];
        byte[] buffer  = new byte[0];
        do {
            LOGGER.log(Level.INFO, "execute reader");
            try {
                buffer  = binaryReader(data);
            } catch (IOException e) {
                LOGGER.log(Level.SEVERE, "failed to read from the input stream");
                return RC.CODE_FAILED_TO_READ;
            }
            if(buffer!=null) {
                consumer.execute(buffer);
            }
        } while(buffer!=null);
        return RC.CODE_SUCCESS;
    }

    public byte[] binaryReader(byte[] data) throws IOException {
        int count = 0;
        byte[] buffer = new byte[bufferSize];
        while((count = inputStream.read(buffer) )!= -1) {
            // LOGGER.log(Level.INFO, "reading from input stream to buffer");
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
}
