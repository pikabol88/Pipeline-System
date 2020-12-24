import ru.spbstu.pipeline.*;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.logging.Level;
import java.util.logging.Logger;

public class BinaryShift implements IExecutor {

    enum BINARY_SHIFT_GRAMMAR {
        SHIFT_SIZE
    }
    public static final String GRAMMAR_SEPARATOR = "=";

    private Configer config;
    private IConsumer consumer;
    private IProducer producer;
    private IMediator mediator;

    private final Logger LOGGER;

    private byte[] processed;
    private final TYPE[] supportedFormats = {TYPE.BYTE};

    static final int BITS = 8;
    static final int MASK = 0xFF;

    public BinaryShift(Logger logger){
        LOGGER=logger;
    }

    @Override
    public RC setConfig(String cfg) {
        config = new Configer(cfg,BINARY_SHIFT_GRAMMAR.values(),GRAMMAR_SEPARATOR, true, LOGGER);
        return config.errorState;
    }

    @Override
    public RC setConsumer(IConsumer iConsumer) {
        if(iConsumer == null) {
            LOGGER.log(Level.SEVERE, "invalid consumer object");
            return RC.CODE_INVALID_ARGUMENT;
        }
        consumer = iConsumer;
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

    @Override
    public RC execute() {
        RC errorState = RC.CODE_SUCCESS;
        processed = (byte[]) mediator.getData();
        if(processed!=null) {
            errorState = binaryShift(processed, Integer.parseInt(config.config.get(BINARY_SHIFT_GRAMMAR.SHIFT_SIZE.toString())));
        }
        if(errorState == RC.CODE_SUCCESS) {
            //LOGGER.log(Level.INFO, "execute " + consumer);
            consumer.execute();
        }
        return errorState;
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
            if(processed == null) return null;
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
