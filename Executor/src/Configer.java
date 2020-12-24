import ru.spbstu.pipeline.RC;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;


public class Configer {

    private static String splitter;
    private final List<String> grammar;
    public HashMap<String,String> config;
    private boolean withGrammar;
    private Logger LOGGER;
    public RC errorState;

    <E extends Enum<E>> Configer(String configFile, Enum<E>[] values, String _splitter, boolean _withGrammar, Logger logger) {
        LOGGER = logger;
        withGrammar = _withGrammar;

        config = new HashMap<>();
        grammar = enumValues(values);
        splitter = _splitter;
        errorState = readConfig(configFile);
    }

    private <E extends Enum<E>> List<String> enumValues(Enum<E>[] values){
        ArrayList<String> str = new ArrayList();
        for (Enum<E> i : values){
            str.add(i.toString());
        }
        return str;
    }

    private RC readConfig(String file) {
        if(file == null){
            return RC.CODE_INVALID_ARGUMENT;
        }
        try {
            BufferedReader br = new BufferedReader(new FileReader(file));
            String line;
            LOGGER.log(Level.INFO, "start parsing config file " + file);
            while((line = br.readLine())!=null) {
                if(withGrammar) parseConfig(line/*.toUpperCase()*/);
                else parseConfigWithoutGrammar(line);
            }
            br.close();
        } catch(IOException ex) {
            LOGGER.log(Level.SEVERE, "failed opening the config file: " + file);
            return RC.CODE_INVALID_INPUT_STREAM;
        }
        return checkConfig();
    }

    public void setWithoutGrammarMode(){
        withGrammar=false;
    }

    private void parseConfig(String line) {
        String[] words =  line.split(splitter);
        LOGGER.log(Level.INFO, "filling config container");
        for(String value: grammar){
            if(value.equals(words[0])){
                config.put(value, words[1]/*.toLowerCase()*/) ;
                break;
            }
        }
    }

    private void parseConfigWithoutGrammar(String line) {
        String[] words =  line.split(splitter);
        LOGGER.log(Level.INFO, "filling config container");
        config.put(words[0], words[1]/*.toLowerCase()*/) ;
    }

    private RC checkConfig(){
        if(withGrammar) {
            if (config.size() < grammar.size()) {
                return RC.CODE_CONFIG_GRAMMAR_ERROR;
            }
            return RC.CODE_SUCCESS;
        }
        return RC.CODE_SUCCESS;
    }

}
