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
        String tuple = "104,223,'Washington, D.C.',4";
        SqlRowParser parser = new SqlRowParser();
        List<String> fields = parser.splitFields(tuple);

        assertEquals(fields.size(), 4);
        assertEquals(fields.get(0), "104");
        assertEquals(fields.get(1), "223");
        assertEquals(fields.get(2), "'Washington, D.C.'");
        assertEquals(fields.get(3), "4");
    }
}
