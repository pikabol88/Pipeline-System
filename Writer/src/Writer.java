import ru.spbstu.pipeline.IExecutable;
import ru.spbstu.pipeline.IWriter;
import ru.spbstu.pipeline.RC;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Writer implements IWriter {

    enum WRITER_GRAMMAR {
        BUFFER_SIZE
    }
    public static final String GRAMMAR_SEPARATOR = "=";

    Configer config;

    IExecutable producer;
    OutputStream outputStream;
    Logger LOGGER;

    int bufferSize;

    public Writer(Logger logger) {
        LOGGER = logger;
    }

    @Override
    public RC setOutputStream(FileOutputStream fos) {
        outputStream = fos;
        return RC.CODE_SUCCESS;
    }

    @Override
    public RC setConfig(String cfg) {
        config = new Configer(cfg, WRITER_GRAMMAR.values(),GRAMMAR_SEPARATOR, true, LOGGER);
        bufferSize = Integer.parseInt(config.config.get(WRITER_GRAMMAR.BUFFER_SIZE.toString()));
        return config.errorState;
    }

    @Override
    public RC setConsumer(IExecutable c) {
        return RC.CODE_SUCCESS;
    }

    @Override
    public RC setProducer(IExecutable p) {
        if(p==null) {
            LOGGER.log(Level.SEVERE, "invalid producer object");
            return RC.CODE_INVALID_ARGUMENT;
        }
        producer = p;
        return RC.CODE_SUCCESS;
    }

    @Override
    public RC execute(byte[] data) {
        LOGGER.log(Level.INFO, "execute writer");
        return binaryWriter(data);
    }


    public RC binaryWriter( byte[] buffer){
        try {
            //LOGGER.log(Level.INFO, "writing to the output stream");
            for(int i = 0; i * bufferSize + bufferSize <= buffer.length - 1; ++i) {
                outputStream.write(buffer, i * this.bufferSize, this.bufferSize);
            }
            outputStream.write(buffer, 0, buffer.length);
        } catch (IOException ex) {
            LOGGER.log(Level.SEVERE, "failed to write to the input stream");
            return RC.CODE_FAILED_TO_WRITE;
        }
        return RC.CODE_SUCCESS;
    }
}
