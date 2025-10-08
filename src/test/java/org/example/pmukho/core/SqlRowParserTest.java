package org.example.pmukho.core;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;



public class SqlRowParserTest {

    @Test
    void splitFields_Basic() {
        String tuple = "0,1,'Alice'";
        SqlRowParser parser = new SqlRowParser();
        String[] fields = parser.splitFields(tuple);

        assertEquals(fields.length, 3);
        assertEquals(fields[0], "0");
        assertEquals(fields[1], "1");
        assertEquals(fields[2], "'Alice'");
    }

    @Test
    void splitFields_Comma_in_string() {
        String tuple = "104,223,'Washington, D.C.'";
        SqlRowParser parser = new SqlRowParser();
        String[] fields = parser.splitFields(tuple);

        assertEquals(fields.length, 3);
        assertEquals(fields[0], "104");
        assertEquals(fields[1], "203");
        assertEquals(fields[2], "'Washington, D.C.'");
    }
}
