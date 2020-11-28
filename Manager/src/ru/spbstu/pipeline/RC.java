package ru.spbstu.pipeline;

public enum RC {
	CODE_SUCCESS("Successful"), // ошибки не произошло
	CODE_INVALID_ARGUMENT("Check the argument!"), // передан невалидный аргумент
	CODE_FAILED_TO_READ("An error occurred during reading from the stream"), // невозможно прочитать из потока
	CODE_FAILED_TO_WRITE("An error occurred during writing to the stream"), // невозможно записать в поток
	CODE_INVALID_INPUT_STREAM("File for reading not found"), // невозможно открыть поток на чтение
	CODE_INVALID_OUTPUT_STREAM("Failed to open file for writing"), // невозможно открыть поток на запись
	CODE_CONFIG_GRAMMAR_ERROR("Error with grammar in config file"), // ошибка в грамматике конфига
	CODE_CONFIG_SEMANTIC_ERROR("Error with semantic in config file"), // семантическая ошибка в конфиге
	CODE_FAILED_PIPELINE_CONSTRUCTION("Error in pipeline creation"); // при конструировании конвейера произошла ошибка

	private final String description;

	RC(String description) {
		this.description = description;
	}

	public String getDescription() {
		return description;
	}
}
