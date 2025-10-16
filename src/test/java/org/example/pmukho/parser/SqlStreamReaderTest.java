package org.example.pmukho.parser;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class SqlStreamReaderTest {

    @Test
    void read_single_basic_insert() throws URISyntaxException {
        SqlStreamReader reader = new SqlStreamReader();
        Path inputFile = Paths.get(getClass().getResource("/single-basic-insert.txt").toURI());

        List<List<String>> output = new ArrayList<>();
        reader.read(inputFile, "my_table", tuple -> {
            output.add(tuple);
        });

        List<List<String>> expected = List.of(
                List.of("0", "-3", "Alice", "3.14"),
                List.of("1", "1", "Bob", "-1.4"));

        assertEquals(expected, output);
    }

    @Test
    void read_multi_basic_insert() throws URISyntaxException {
        SqlStreamReader reader = new SqlStreamReader();
        Path inputFile = Paths.get(getClass().getResource("/multi-basic-insert.txt").toURI());

        List<List<String>> output = new ArrayList<>();
        reader.read(inputFile, "my_table", tuple -> {
            output.add(tuple);
        });

        List<List<String>> expected = List.of(
                List.of("0", "-3", "Alice", "3.14"),
                List.of("1", "1", "Bob", "-1.4"),
                List.of("2", "-4", "Charlie", "1.9"),
                List.of("8", "21", "David", "1.49"),
                List.of("100", "3", "Edna", "8.91123"));

        assertEquals(expected, output);
    }

    @Test
    void read_escape_single_quote_insert() throws URISyntaxException {
        SqlStreamReader reader = new SqlStreamReader();
        Path inputFile = Paths.get(getClass().getResource("/escape-single-quote-insert.txt").toURI());

        List<List<String>> output = new ArrayList<>();
        reader.read(inputFile, "my_table", tuple -> {
            output.add(tuple);
        });

        List<List<String>> expected = List.of(
                List.of("0", "-3", "Alice", "3.14"),
                List.of("1", "1", "O'Reilly", "-1.4"),
                List.of("18", "19", "Charlie", "28.9"),
                List.of("1", "1", "Nyong'o", "1.0"));

        assertEquals(expected, output);
    }

    @Test
    void read_parentheses_insert() throws URISyntaxException {
        SqlStreamReader reader = new SqlStreamReader();
        Path inputFile = Paths.get(getClass().getResource("/parentheses-insert.txt").toURI());

        List<List<String>> output = new ArrayList<>();
        reader.read(inputFile, "my_table", tuple -> {
            output.add(tuple);
        });

        List<List<String>> expected = List.of(
                List.of("0", "-3", "Alice", "3.14"),
                List.of("1", "1", "Bob (Robert)", "-1.4"),
                List.of("2", "14", "Charlie (Charles)", "2.3"));

        assertEquals(expected, output);
    }
}
