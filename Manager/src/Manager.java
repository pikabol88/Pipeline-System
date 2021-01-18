import ru.spbstu.pipeline.*;
import ru.spbstu.pipeline.RC;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Manager implements IConfigurable {

    enum CONFIG_GRAMMAR {
        READER_CFG,
        READER_NAME,
        EXECUTORS_FILE,
        WRITER_CFG,
        WRITER_NAME,
        INPUT_FILE,
        OUTPUT_FILE,
    }

    enum EXECUTORS_CONFIG_GRAMMAR {
        EXECUTOR_NAME,
        EXECUTOR_CFG
    }
    public static final String GRAMMAR_SEPARATOR = "=";

    private final Logger LOGGER;

    private Configer executorsConfig;
    private Configer config;

    private final String mainCfg;
    private String executorsCfg;

    private FileInputStream input;
    private FileOutputStream output;

    private RC errorState;

    private IExecutor[] allExecutors;
    private IReader reader;
    private IWriter writer;

    public boolean checkSuccess() {
        if (errorState!=RC.CODE_SUCCESS) {
            LOGGER.log(Level.SEVERE, ErrorDescription.getDescription(errorState));
            return  false;
        }
        return  true;
    }

    Manager(Logger logger, String configName) {
        mainCfg=configName;
        LOGGER = logger;
        errorState = setConfig(configName);
        if(!checkSuccess()){return;}
        errorState = setExecutorsConfig(executorsCfg);
        if(!checkSuccess()){return;}
        errorState = setReader();
        if(!checkSuccess()){return;}
        errorState = setWriter();
        if(!checkSuccess()){return;}
        errorState = CreatePipeline(executorsConfig.config);
    }


    @Override
    public RC setConfig(String cfg) {
        config = new Configer(mainCfg, CONFIG_GRAMMAR.values(), GRAMMAR_SEPARATOR, true, LOGGER);
        executorsCfg = config.config.get(CONFIG_GRAMMAR.EXECUTORS_FILE.toString());
        return config.errorState;
    }

    private RC setExecutorsConfig(String cfg){
        executorsConfig = new Configer(cfg, EXECUTORS_CONFIG_GRAMMAR.values(),GRAMMAR_SEPARATOR, false,LOGGER);
        return executorsConfig.errorState;
    }

    private RC setReader(){
        reader = (IReader) CreateExecutable(config.config.get(CONFIG_GRAMMAR.READER_NAME.toString()),config.config.get(CONFIG_GRAMMAR.READER_CFG.toString()));
        try {
            LOGGER.log(Level.INFO, "opening input stream");
            input = new FileInputStream(config.config.get(CONFIG_GRAMMAR.INPUT_FILE.toString()));
        } catch (IOException ex) {
            LOGGER.log(Level.SEVERE, "failed to create the input stream");
            return RC.CODE_INVALID_INPUT_STREAM;
        }
        reader.setInputStream(input);
        return RC.CODE_SUCCESS;
    }

    private RC setWriter() {
        writer = (IWriter) CreateExecutable(config.config.get(CONFIG_GRAMMAR.WRITER_NAME.toString()),config.config.get(CONFIG_GRAMMAR.WRITER_CFG.toString()));
        try {
            LOGGER.log(Level.INFO, "opening output stream");
            output = new FileOutputStream(config.config.get(CONFIG_GRAMMAR.OUTPUT_FILE.toString()));
        } catch (IOException ex) {
            LOGGER.log(Level.SEVERE, "failed to create the input stream");
            return RC.CODE_INVALID_OUTPUT_STREAM;
        }
        writer.setOutputStream(output);
        return RC.CODE_SUCCESS;
    }


    private IConfigurable CreateExecutable(String className, String configPath) {
        IConfigurable pipelineProcess;
        try {
            LOGGER.log(Level.INFO, "trying to create executor with name " + className);
            Class obj = Class.forName(className);
            Class[] construct = new Class[]{Logger.class};
            pipelineProcess = (IConfigurable) obj.getConstructor(construct).newInstance(this.LOGGER);
            if (pipelineProcess.setConfig(configPath)==RC.CODE_SUCCESS){
                return pipelineProcess;
            }
            return  null;
        } catch (NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException | ClassNotFoundException ex) {
            LOGGER.log(Level.WARNING, "failed to create executor with name " + className);
            return  null;
        }
    }

    private RC CreatePipeline(Map<String, String> executors){
        allExecutors = new IExecutor[executors.size()];
        int numOfExecutors = executors.size();
        RC state;
        int i = 0;

        for (Map.Entry<String, String> entry: executors.entrySet()) {
            String className = entry.getKey();
            if (className.contains(config.repeatInd)){
                className = className.substring(0, className.length() - config.repeatInd.length() - 1);
            }
            String configName = entry.getValue();
            allExecutors[i] = (IExecutor) CreateExecutable(className, configName);
            if (allExecutors[i]==null){
                LOGGER.log(Level.SEVERE, "amount of executors is zero");
                return RC.CODE_FAILED_PIPELINE_CONSTRUCTION;
            }
            i++;
        }
        if (numOfExecutors == 0){
            state = writer.setProducer(reader);
            reader.addNotifier(writer.getNotifier());
        } else {
            IExecutor curExecutor = allExecutors[0];
            curExecutor.setProducer(reader);
            reader.addNotifier(curExecutor.getNotifier());
            for (i = 1; i< numOfExecutors; i++) {
                LOGGER.log(Level.SEVERE, "determinate the oder for executor: " + curExecutor);
                state =  allExecutors[i].setProducer(curExecutor);
                curExecutor.addNotifier(allExecutors[i].getNotifier());
                if(state!=RC.CODE_SUCCESS){  return state; }
                curExecutor=allExecutors[i];
            }
            state = writer.setProducer(curExecutor);
            curExecutor.addNotifier(writer.getNotifier());
        }
        if(state!=RC.CODE_SUCCESS){  return state; }
        StringBuilder str = new StringBuilder("pipleline was successfully built, the worker order:");
        for(i = 0;i<allExecutors.length;i++) str.append("\n\t").append(i + 1).append(". ").append(allExecutors[i]);
        LOGGER.log(Level.INFO, str.toString());
        return RC.CODE_SUCCESS;
    }

    public RC closeStreams(){
        LOGGER.log(Level.INFO, "start closing streams");
        try {
            if(input!=null) { input.close(); }
        } catch (IOException ex) {
            LOGGER.log(Level.SEVERE, "failed to close the input stream");
            return RC.CODE_INVALID_INPUT_STREAM;
        }
        try {
            if(output!=null) { output.close(); }
        } catch (IOException ex) {
            LOGGER.log(Level.SEVERE, "failed to close the output stream");
            return RC.CODE_INVALID_OUTPUT_STREAM;
        }
        return RC.CODE_SUCCESS;
    }

    public void Run() throws InterruptedException {
        ArrayList<Thread> threads = new ArrayList<>();
        LOGGER.log(Level.INFO, "start running pipeline");
        LOGGER.log(Level.INFO, "start running reader thread");
        Thread readThread = new Thread(reader);
        threads.add(readThread);

        for (IConfigurable allExecutor : allExecutors) {
            LOGGER.log(Level.INFO, "start running executor "+ Arrays.toString(allExecutors) +" thread");
            Thread executorThread = new Thread((Runnable) allExecutor);
            threads.add(executorThread);
        }
        LOGGER.log(Level.INFO, "start running writer thread");
        Thread writeThread = new Thread(writer);
        threads.add(writeThread);

        for(Thread th:threads){
            th.start();
            th.join();
        }
       // writeThread.join();
        if(closeStreams()!=RC.CODE_SUCCESS){
            LOGGER.log(Level.SEVERE, "failed to close the stream");
        } else {
            LOGGER.log(Level.INFO, "pipeline running successfully finished");
        }
    }

}