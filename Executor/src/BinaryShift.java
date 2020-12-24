import ru.spbstu.pipeline.IExecutable;
import ru.spbstu.pipeline.IExecutor;
import ru.spbstu.pipeline.RC;

import java.util.logging.Level;
import java.util.logging.Logger;

public class BinaryShift implements IExecutor {

    enum BINARY_SHIFT_GRAMMAR {
        SHIFT_SIZE
    }
    public static final String GRAMMAR_SEPARATOR = "=";

    private Configer config;
    static final int BITS = 8;
    static final int MASK = 0xFF;
    private IExecutable consumer;
    private IExecutable producer;
    private final Logger LOGGER;

    public BinaryShift(Logger logger){
        LOGGER=logger;
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
        if(p==null) {
            LOGGER.log(Level.SEVERE, "invalid producer object");
            return RC.CODE_INVALID_ARGUMENT;
        }
        return RC.CODE_SUCCESS;
    }

    static public RC binaryShift(byte[] bytes, int shift){
        if(shift>0){
            return rightShift(bytes, shift%BITS);
        } else if(shift<0){
            return  leftShift(bytes,Math.abs(shift%BITS));
        }
        return RC.CODE_SUCCESS;
    }


    static private RC rightShift(byte[] bytes, int shift){
        if(bytes == null){
            return RC.CODE_INVALID_ARGUMENT;
        }
        for(int i = 0;i<bytes.length;i++){
            int x = bytes[i] & MASK; // Побитовое И с маской 11111111 для устранения расширения знака при приведении отрицательных значений
            bytes[i]= (byte) ((x >> shift) | (x<< (BITS - shift)));
        }
        return RC.CODE_SUCCESS;
    }

    static private RC leftShift(byte[] bytes, int shift){
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
    public RC execute(byte[] data) {
        LOGGER.log(Level.INFO, "execute binary shift");
        if(data==null){
            LOGGER.log(Level.SEVERE, "invalid data argument");
            return RC.CODE_INVALID_ARGUMENT;
        }
        RC errorState = binaryShift(data, Integer.parseInt(config.config.get(BINARY_SHIFT_GRAMMAR.SHIFT_SIZE.toString())));
        if(errorState==RC.CODE_SUCCESS) {
            consumer.execute(data);
        }
        return errorState;
    }

    @Override
    public RC setConfig(String cfg) {
        config = new Configer(cfg,BINARY_SHIFT_GRAMMAR.values(),GRAMMAR_SEPARATOR, true, LOGGER);
        return config.errorState;
    }

}
