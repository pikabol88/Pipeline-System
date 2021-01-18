import ru.spbstu.pipeline.*;
import ru.spbstu.pipeline.RC;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Writer implements IWriter {

    enum WRITER_GRAMMAR {
        BUFFER_SIZE
    }
    public static final String GRAMMAR_SEPARATOR = "=";

    Configer config;
    private final TYPE[] supportedFormats={TYPE.BYTE};

    private IProducer producer;
    private IMediator mediator;
    private OutputStream outputStream;
    private final Logger LOGGER;

    private int bufferSize;
    private int currentSize = 0;
    private byte[] buffer;

    public ArrayList<Integer> accepted_chunks = new ArrayList<>();
    private int currentChunk = 0;
    private  boolean isDone = false;

    public Writer(Logger logger) {
        LOGGER = logger;
    }

    @Override
    public void run() {
        RC state=RC.CODE_SUCCESS;
        do {
            if(accepted_chunks.contains(currentChunk)){
                state = execute();
                if (state!=RC.CODE_SUCCESS) {
                    LOGGER.log(Level.SEVERE, ErrorDescription.getDescription(state));
                }
                currentChunk++;
            }
        } while (!isDone);
        LOGGER.log(Level.INFO, "writer throw processing is finished");
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
        buffer = new byte[bufferSize];
        return config.errorState;
    }

    @Override
    public RC addNotifier(INotifier iNotifier) {
        return null;
    }

    @Override
    public RC setProducer(IProducer iProducer)  {
        if(iProducer==null) {
            LOGGER.log(Level.SEVERE, "invalid producer object");
            return RC.CODE_INVALID_ARGUMENT;
        }
        producer = iProducer;
        TYPE type = typeIntersection();
        if(type!=null) {
            mediator = producer.getMediator(type);
            return RC.CODE_SUCCESS;
        }
        return RC.CODE_FAILED_PIPELINE_CONSTRUCTION;
    }

    private TYPE typeIntersection(){
        TYPE[] target = (producer.getOutputTypes());
        for (TYPE el:supportedFormats) {
            for(TYPE target_el:target){
                if(target_el == el){
                    return el;
                }
            }
        }
        LOGGER.log(Level.SEVERE, "Executor can't get supported format of data");
        return null;
    }


    public RC execute() {
        byte[] data = (byte[])mediator.getData(currentChunk);
       // System.out.println("write " + currentChunk + " = " + Arrays.toString(data));
        if (data == null){
            isDone = true;
        }
        return binaryWriter(data);
    }

    @Override
    public INotifier getNotifier() {
        return new Notifier();
    }

    public RC binaryWriter(byte[] data) {
        try {
            if(data == null) {
                outputStream.write(buffer, 0, currentSize );
            } else if(data.length == 0){
                outputStream.write(buffer, 0, currentSize );
            } else if(data.length==bufferSize) { outputStream.write(data, 0, data.length);
            } else {
                for (byte b : data) {
                    buffer[currentSize++] = b;
                    if (currentSize == bufferSize) {
                        outputStream.write(buffer, 0, buffer.length);
                        currentSize = 0;
                    }
                }
            }
        } catch (IOException ex) {
            LOGGER.log(Level.SEVERE, "failed to write to the input stream");
            return RC.CODE_FAILED_TO_WRITE;
        }
        return RC.CODE_SUCCESS;
    }


    private class Notifier implements INotifier {
        @Override
        public RC notify(int idChunk) {
            if (accepted_chunks.contains(idChunk)) {
                return RC.CODE_WARNING_CHUNK_ALREADY_TAKEN;
            } else {
                accepted_chunks.add(idChunk);
                return RC.CODE_SUCCESS;
            }
        }
    }
}
