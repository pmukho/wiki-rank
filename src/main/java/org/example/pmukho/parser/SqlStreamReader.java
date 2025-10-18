package org.example.pmukho.parser;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.zip.GZIPInputStream;

public class SqlStreamReader<T> {

    public void read(Path path, String tableName, Function<List<String>, T> builder, Consumer<T> consumer) {

        try (InputStream fileStream = Files.newInputStream(path);
                InputStream in = path.toString().endsWith(".gz") ? new GZIPInputStream(fileStream) : fileStream;
                BufferedReader br = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {

            ParserContext<T> parser = new ParserContext<>(br, tableName, builder, consumer);
            parser.parse();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

interface ParserState<T> {
    void handleChar(ParserContext<T> context, char ch) throws IOException;
}

class ParserContext<T> {
    // fields used to track state and info related to state
    private ParserState<T> state;
    private String currentLine;
    private int index;
    // fields set by constructor
    private BufferedReader reader;
    private Consumer<T> consumer;
    private Function<List<String>, T> builder;
    private String tableName;
    // fields used to hold processed input segments
    StringBuilder buffer = new StringBuilder();
    List<String> fields = new ArrayList<>();

    ParserContext(BufferedReader reader, String tableName, Function<List<String>, T> builder,
            Consumer<T> consumer) {
        this.reader = reader;
        this.builder = builder;
        this.consumer = consumer;
        this.tableName = tableName;
    }

    void setState(ParserState<T> state) {
        this.state = state;
    }

    void parse() throws IOException {
        String line;
        while ((line = reader.readLine()) != null) {
            String prefix = "INSERT INTO `" + tableName + "` VALUES ";
            String suffix = ";";
            if (line.startsWith(prefix) && line.endsWith(suffix)) {
                parseLine(line.substring(
                        prefix.length(),
                        line.length() - suffix.length()));
            }
        }
    }

    void parseLine(String insertLine) throws IOException {
        currentLine = insertLine;
        index = 0;
        setState(new StartState<>());

        for (; index < currentLine.length(); index++) {
            state.handleChar(this, insertLine.charAt(index));
        }
    }

    char peek() {
        if (index >= currentLine.length()) {
            return (char) -1;
        }
        return currentLine.charAt(index + 1);
    }

    void emit() {
        T record = builder.apply(fields);
        fields.clear();
        consumer.accept(record);
    }
}

class StartState<T> implements ParserState<T> {

    @Override
    public void handleChar(ParserContext<T> context, char ch) throws IOException {
        if (ch == '(') {
            context.setState(new FieldState<>());
        }
    }

}

class FieldState<T> implements ParserState<T> {

    @Override
    public void handleChar(ParserContext<T> context, char ch) throws IOException {
        if (ch == '\'') {
            context.setState(new StringState<>());
        } else if (ch == ',') {
            context.fields.add(context.buffer.toString());
            context.buffer.setLength(0);
        } else if (ch == ')') {
            context.fields.add(context.buffer.toString());
            context.buffer.setLength(0);
            context.emit();
            context.setState(new EmitState<>());
        } else {
            context.buffer.append(ch);
        }
    }

}

class StringState<T> implements ParserState<T> {

    @Override
    public void handleChar(ParserContext<T> context, char ch) throws IOException {
        // refer to link below to see special cases involving string literals
        // https://dev.mysql.com/doc/refman/8.4/en/string-literals.html

        char lookAhead = context.peek();
        if (ch == '\\') {
            context.setState(new EscapeState<>());
        } else if (ch == '\'' && (lookAhead == ',' || lookAhead == ')')) {
            context.setState(new FieldState<>());
        } else if (ch == '\'' && lookAhead == '\'') { // MySQL uses \'\' for literal char \'
            return; // "eat" current \'
        } else {
            context.buffer.append(ch);
        }
    }

}

class EscapeState<T> implements ParserState<T> {

    @Override
    public void handleChar(ParserContext<T> context, char ch) throws IOException {
        context.buffer.append(ch);
        context.setState(new StringState<>());
    }

}

class EmitState<T> implements ParserState<T> {

    @Override
    public void handleChar(ParserContext<T> context, char ch) throws IOException {
        // emit should be done before transitioning to this state
        if (ch == ',') {
            context.setState(new StartState<>());
        } else if (ch == ';') {
            context.setState(new EndState<>());
        }
    }

}

class EndState<T> implements ParserState<T> {

    @Override
    public void handleChar(ParserContext<T> context, char ch) throws IOException {
        return;
    }

}