package org.example.pmukho.parser;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

public class SqlStreamReader {

    public void read(Path path, TupleHandler handler) {

        try (InputStream fileStream = Files.newInputStream(path);
                InputStream in = path.toString().endsWith(".gz") ? new GZIPInputStream(fileStream) : fileStream;
                BufferedReader br = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {

            ParserContext parser = new ParserContext(br, handler);
            parser.parse();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

interface ParserState {
    void handleChar(ParserContext context, char ch) throws IOException;
}

class ParserContext {
    private ParserState state = new SeekInsertState();
    StringBuilder buffer = new StringBuilder();
    private BufferedReader reader;
    private TupleHandler handler;

    ParserContext(BufferedReader reader, TupleHandler handler) {
        this.reader = reader;
        this.handler = handler;
    }

    void setState(ParserState state) {
        this.state = state;
    }

    void parse() throws IOException {
        int ch;
        while ((ch = reader.read()) != -1) {
            state.handleChar(this, (char) ch);
        }
    }

    char peek() throws IOException {
        reader.mark(1);
        int ch = reader.read();
        reader.reset();
        return (char) ch;
    }

    // only used to force a read of one character
    void skip() throws IOException {
        reader.read();
    }

    void emitTuple() {
        String tuple = buffer.toString().trim();
        List<String> fields = new ArrayList<>();

        StringBuilder currField = new StringBuilder();
        boolean inString = false;
        for (int i = 1; i < tuple.length()-1; i++) {
            char ch = tuple.charAt(i);

            if (ch == '\"') {
                inString = !inString;
            } else if (ch == ',' && !inString) {
                fields.add(currField.toString());
                currField.setLength(0);
            } else {
                currField.append(ch);
            }
        }
        fields.add(currField.toString());
        currField.setLength(0);

        handler.onTuple(fields);
        buffer.setLength(0);
    }
}

class SeekInsertState implements ParserState {

    @Override
    public void handleChar(ParserContext context, char ch) throws IOException {
        context.buffer.append(ch);
        if (context.buffer.length() > 20)
            context.buffer.deleteCharAt(0);

        if (context.buffer.toString().trim().toUpperCase().endsWith("VALUES")) {
            context.setState(new BuildTupleState());
            context.buffer.setLength(0);
        }
    }
}

class BuildTupleState implements ParserState {

    @Override
    public void handleChar(ParserContext context, char ch) throws IOException {
        char lookAhead = context.peek();
        if (ch == '\'') {
            context.buffer.append('\"');
            context.setState(new BuildStringState());
        } else if (ch == ')' && lookAhead == ',') { // reached end of tuple
            context.buffer.append(')');
            context.emitTuple();
            context.skip();
        } else if (ch == ')' && lookAhead == ';') {
            context.buffer.append(')');
            context.emitTuple();
            context.skip();
            context.setState(new SeekInsertState());
        } else {
            context.buffer.append(ch);
        }
    }
}

class BuildStringState implements ParserState {

    @Override
    public void handleChar(ParserContext context, char ch) throws IOException {
        char lookAhead = context.peek();
        if (ch == '\'' && lookAhead == '\'') { // MySQL uses '' to imitate \'
            context.buffer.append('\'');
            context.skip();
        } else if (ch == '\'' && lookAhead != '\'') {
            context.buffer.append('\"');
            context.setState(new BuildTupleState());
        } else {
            context.buffer.append(ch);
        }
    }
}