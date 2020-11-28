//import com.Viktor.main.Reader;
//import mekhails.reader.ByteReader;
import ru.spbstu.pipeline.*;
import ru.spbstu.pipeline.RC;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
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

    private Configer exConfig;
    private Configer config;

    private String mainCfg;
    private String executorsCfg;

    private FileInputStream input;
    private FileOutputStream output;

    private RC errorState = RC.CODE_SUCCESS;

    private IExecutable[] allExecutors;
    private int numOfExecutors;
    private IReader reader ;
    private IWriter writer;

    byte[] bytes;

    public boolean checkSuccess() {
        if (errorState!=RC.CODE_SUCCESS) {
            LOGGER.log(Level.SEVERE, errorState.getDescription());
            return  false;
        }
        return  true;
    }

    Manager(Logger logger, String configName) {
        mainCfg=configName;
        LOGGER = logger;
        errorState = setConfig(configName);
        if(checkSuccess()==false){return;}
        errorState = setExecutorsConfig(executorsCfg);
        if(checkSuccess()==false){return;}
        errorState = setReader();
        if(checkSuccess()==false){return;}
        errorState = setWriter();
        if(checkSuccess()==false){return;}
        errorState = CreatePipeline(exConfig.config);
        if(checkSuccess()==false){return;}
    }


    @Override
    public RC setConfig(String cfg) {
        config = new Configer(mainCfg, CONFIG_GRAMMAR.values(), GRAMMAR_SEPARATOR, true, LOGGER);
        executorsCfg = config.config.get(CONFIG_GRAMMAR.EXECUTORS_FILE.toString());
        return config.errorState;
    }

    private RC setExecutorsConfig(String cfg){
        exConfig = new Configer(cfg, EXECUTORS_CONFIG_GRAMMAR.values(),GRAMMAR_SEPARATOR, false,LOGGER);
        return exConfig.errorState;
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

    private RC setWriter(){
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


    private IExecutable CreateExecutable(String className, String configPath) {
        IExecutable pipelineProcess;
        try {
            LOGGER.log(Level.INFO, "trying to create executor with name " + className);
            Class obj = Class.forName(className);
            Class[] construct = new Class[]{Logger.class};
            pipelineProcess = (IExecutable)obj.getConstructor(construct).newInstance(this.LOGGER);
            if (((IConfigurable)pipelineProcess).setConfig(configPath)==RC.CODE_SUCCESS){
                return pipelineProcess;
            }
            return  null;
        } catch (NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException | ClassNotFoundException ex) {
            LOGGER.log(Level.WARNING, "failed to create executor with name " + className);
            return  null;
        }
    }

    private RC CreatePipeline(Map<String, String> executors){
        allExecutors = new IExecutable[executors.size()];
        numOfExecutors = executors.size();
        RC state;
        int i = 0;

        for (Map.Entry<String, String> entry: executors.entrySet()) {
            String className = entry.getKey();
            String configName = entry.getValue();
            allExecutors[i] = CreateExecutable(className, configName);
            if (allExecutors[i]==null){
                LOGGER.log(Level.SEVERE, "amount of executors is zero");
                return RC.CODE_FAILED_PIPELINE_CONSTRUCTION;
            }
            i++;
        }

        if (numOfExecutors == 0){
            reader.setConsumer(writer);
            writer.setProducer(reader);
        } else {
            IExecutable curExecutor = allExecutors[0];
            reader.setConsumer(curExecutor);
            for (i = 1; i<numOfExecutors;i++) {
                LOGGER.log(Level.SEVERE, "determinate the oder for executor: " + curExecutor);
                state = ((IPipelineStep)curExecutor).setConsumer(allExecutors[i]);
                if(state!=RC.CODE_SUCCESS){  return state; }
                state = ((IPipelineStep)allExecutors[i]).setProducer(curExecutor);
                if(state!=RC.CODE_SUCCESS){  return state; }
                curExecutor=allExecutors[i];
            }
            state = ((IPipelineStep)curExecutor).setConsumer(writer);
            if(state!=RC.CODE_SUCCESS){  return state; }
            state = writer.setProducer(curExecutor);
            if(state!=RC.CODE_SUCCESS){  return state; }
        }
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

    public void Run() {
        LOGGER.log(Level.INFO, "start running pipeline");
        reader.execute(bytes);
        if(closeStreams()!=RC.CODE_SUCCESS){
            LOGGER.log(Level.SEVERE, "failed to close the stream");
        } else {
            LOGGER.log(Level.INFO, "pipeline running successfully finished");
        }
    }

}