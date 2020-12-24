import ru.spbstu.pipeline.RC;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;


public class Main {
    public static final Logger LOGGER = Logger.getLogger(Logger.class.getName());
    public static final String logConfig = "/logging.properties";

    public static void main(String[] args) throws IOException {
        try {
            LogManager.getLogManager().readConfiguration(Main.class.getResourceAsStream(logConfig));
        } catch (NullPointerException ex){
            System.out.println(ErrorDescription.getDescription(RC.CODE_INVALID_ARGUMENT));
        }
        if(args!=null) {
            Manager manager = new Manager(LOGGER, args[0]);
            if(manager.checkSuccess()) {
                manager.Run();
            } else {
                LOGGER.log(Level.SEVERE, "ERROR! Check log to find more detailed information");
            }
        }
        else {
            System.out.println(ErrorDescription.getDescription(RC.CODE_INVALID_ARGUMENT));
            LOGGER.log(Level.SEVERE, ErrorDescription.getDescription(RC.CODE_INVALID_ARGUMENT));
        }
    }
}
