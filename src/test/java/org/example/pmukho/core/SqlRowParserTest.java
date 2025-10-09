package org.example.pmukho.core;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.util.List;

public class SqlRowParserTest {

    @Test
    void splitFields_Basic() {
        String tuple = "0,1,'Alice'";
        SqlRowParser parser = new SqlRowParser();
        List<String> fields = parser.splitFields(tuple);

        assertEquals(fields.size(), 3);
        assertEquals(fields.get(0), "0");
        assertEquals(fields.get(1), "1");
        assertEquals(fields.get(2), "'Alice'");
    }

    @Test
    void splitFields_Comma_in_string() {
        String tuple = "104,223,'Washington,_D.C.',4";
        SqlRowParser parser = new SqlRowParser();
        List<String> fields = parser.splitFields(tuple);

        assertEquals(fields.size(), 4);
        assertEquals(fields.get(0), "104");
        assertEquals(fields.get(1), "223");
        assertEquals(fields.get(2), "'Washington,_D.C.'");
        assertEquals(fields.get(3), "4");
    }

    @Test
    void parseInsert_Basic() {
        /*
         * Test string is a modified excerpt from
         * https://meta.wikimedia.org/wiki/Data_dumps/Dump_format
         */
        String insertStmt = "INSERT INTO `page` VALUES "
                + "(1,0,'accueil','',0,0,0.228483556876),"
                + "(4,8,'Disclaimers','',0,0,0.436798504291),"
                + "(5,8,'Disclaimerpage','',0,1,0.562675562949)";
        SqlRowParser parser = new SqlRowParser();
        List<List<String>> rows = parser.parseInsert(insertStmt);

        List<List<String>> expectedOutput = List.of(
                List.of("1", "0", "'accueil'", "''", "0", "0", "0.228483556876"),
                List.of("4", "8", "'Disclaimers'", "''", "0", "0", "0.436798504291"),
                List.of("5", "8", "'Disclaimerpage'", "''", "0", "1", "0.562675562949"));

        assertEquals(rows, expectedOutput);
    }

    @Test
    void parseInsert_Parentheses_in_String() {
        String insertStmt = "INSERT INTO `page` VALUES "
                + "(1,0,'accueil','',0,0,0.228483556876),"
                + "(4,8,'Disclaimers','',0,0,0.436798504291),"
                + "(1,2,'Dune_(novel)','',0,1,0.562675562949)";
        SqlRowParser parser = new SqlRowParser();
        List<List<String>> rows = parser.parseInsert(insertStmt);

        List<List<String>> expectedOutput = List.of(
                List.of("1", "0", "'accueil'", "''", "0", "0", "0.228483556876"),
                List.of("4", "8", "'Disclaimers'", "''", "0", "0", "0.436798504291"),
                List.of("1", "2", "'Dune_(novel)'", "''", "0", "1", "0.562675562949"));

        assertEquals(rows, expectedOutput);
    }
}
