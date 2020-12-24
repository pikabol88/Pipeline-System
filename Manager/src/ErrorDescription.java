import ru.spbstu.pipeline.RC;

public  class ErrorDescription {
    static String getDescription(RC error){
        switch (error){
            case CODE_SUCCESS:
                return ("Successful");
            case CODE_INVALID_ARGUMENT:
                return ("Check the argument!");
            case CODE_FAILED_TO_READ:
                return ("An error occurred during reading from the stream");
            case CODE_FAILED_TO_WRITE:
                return ("An error occurred during writing to the stream");
            case CODE_INVALID_INPUT_STREAM:
                return ("File for reading not found");
            case CODE_INVALID_OUTPUT_STREAM:
                return ("Failed to open file for writing");
            case CODE_CONFIG_GRAMMAR_ERROR:
                return ("Error with grammar in config file");
            case CODE_CONFIG_SEMANTIC_ERROR:
                return ("Error with semantic in config file");
            case CODE_FAILED_PIPELINE_CONSTRUCTION:
                return ("Error in pipeline creation");
            default:
                return null;
        }
    }
}
