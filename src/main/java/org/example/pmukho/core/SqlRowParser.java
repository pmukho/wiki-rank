package org.example.pmukho.core;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.*;

public class SqlRowParser {
    private static final Pattern VALUES_PATTERN = Pattern.compile("\\(([^()]*(?:\\([^()]*\\)[^()]*)*)\\)");

    public List<String> parseInsert(String line) {
        Matcher matcher = VALUES_PATTERN.matcher(line);
        while (matcher.find()) {
            String tuple = matcher.group(1);

            /*
             * TODO: split fields
             * - handle edge cases with , in strings
             */
        }

        return new ArrayList<String>();
    }

    public String[] splitFields(String tupleString) {
        return tupleString.split(",");
    }
}