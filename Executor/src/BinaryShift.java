import ru.spbstu.pipeline.*;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

public class BinaryShift implements IExecutor {

    enum BINARY_SHIFT_GRAMMAR {
        SHIFT_SIZE
    }
    public static final String GRAMMAR_SEPARATOR = "=";

    private Configer config;
    private IProducer producer;
    private IMediator mediator;
    private INotifier notifier;

    private final Logger LOGGER;

    private final TYPE[] supportedFormats = {TYPE.BYTE};

    private final HashMap<Integer, byte[]> chunks = new HashMap<>();
    private final ArrayList<Integer> accepted_chunks = new ArrayList<>();
    private int currentChunk = 0;
    private boolean isDone = false;

    static final int BITS = 8;
    static final int MASK = 0xFF;

    public BinaryShift(Logger logger){
        LOGGER=logger;
    }

    @Override
    public void run() {
        RC state;
        do {
            if(accepted_chunks.contains(currentChunk)){
                state = execute();
                if (state!=RC.CODE_SUCCESS) {
                    LOGGER.log(Level.SEVERE, ErrorDescription.getDescription(state));
                    break;
                }
                notifier.notify(currentChunk);
                currentChunk++;
            }
        } while (!isDone);
        LOGGER.log(Level.INFO, "binaryShift throw processing is finished");
    }

    @Override
    public RC setConfig(String cfg) {
        config = new Configer(cfg,BINARY_SHIFT_GRAMMAR.values(),GRAMMAR_SEPARATOR, true, LOGGER);
        return config.errorState;
    }

    @Override
    public RC addNotifier(INotifier iNotifier) {
        notifier = iNotifier;
        return RC.CODE_SUCCESS;
    }

    @Override
    public RC setProducer(IProducer iProducer) {
        if(iProducer == null) {
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


    public RC execute() {
        RC errorState = RC.CODE_SUCCESS;
        byte[] input = (byte[]) mediator.getData(currentChunk);
       // System.out.println("exec " + currentChunk + " = " + Arrays.toString(input));
        if(input !=null) {
            errorState = binaryShift(input, Integer.parseInt(config.config.get(BINARY_SHIFT_GRAMMAR.SHIFT_SIZE.toString())));
            chunks.put(currentChunk, input);
        } else isDone = true;
        return errorState;
    }

    @Override
    public INotifier getNotifier() {
        return new Notifier();
    }

    private RC binaryShift(byte[] bytes, int shift){
        if(shift>0){
            return rightShift(bytes, shift%BITS);
        } else if(shift<0){
            return  leftShift(bytes,Math.abs(shift%BITS));
        }
        return RC.CODE_SUCCESS;
    }

    private RC rightShift(byte[] bytes, int shift){
        if(bytes == null){
            return RC.CODE_INVALID_ARGUMENT;
        }
        for(int i = 0;i<bytes.length;i++){
            int x = bytes[i] & MASK; // Побитовое И с маской 11111111 для устранения расширения знака при приведении отрицательных значений
            bytes[i]= (byte) ((x >> shift) | (x<< (BITS - shift)));
        }
        return RC.CODE_SUCCESS;
    }

    private RC leftShift(byte[] bytes, int shift){
        if(bytes == null){
            return RC.CODE_INVALID_ARGUMENT;
        }
        for(int i = 0;i<bytes.length;i++) {
            int x = bytes[i] & MASK;
            bytes[i] = (byte) ((x << shift) | (x >> (BITS - shift)));
        }
        return RC.CODE_SUCCESS;
    }

    private class Notifier implements INotifier {
        public RC notify(int idChunk) {
            if(accepted_chunks.contains(idChunk)){
                return RC.CODE_WARNING_CHUNK_ALREADY_TAKEN;
            } else accepted_chunks.add(idChunk);
            return RC.CODE_SUCCESS;
        }
    }

    @Override
    public IMediator getMediator(TYPE type) {
        IMediator obj;
        switch (type){
            case BYTE :
                obj = new ByteMediator();
                break;
            case CHAR :
                obj = new CharMediator();
                break;
            case SHORT :
                obj = new ShortMediator();
                break;
            default : throw new IllegalStateException("Unexpected value: " + type);
        }
        return obj;
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

    @Override
    public TYPE[] getOutputTypes() {
        return new TYPE[]{TYPE.SHORT,TYPE.CHAR, TYPE.BYTE};
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
            return  null;
        }
    }

}
