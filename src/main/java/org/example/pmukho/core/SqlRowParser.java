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

    public List<String> splitFields(String tupleString) {
        StringBuilder currField = new StringBuilder();
        List<String> fields = new ArrayList<>();
        boolean inQuote = false;

        for (int i = 0; i < tupleString.length(); i++) {
            char c = tupleString.charAt(i);

            if (c == ',' && !inQuote) {
                fields.add(currField.toString());
                currField.setLength(0);
                continue;
            } else if (c == '\'')
                inQuote = !inQuote;

            currField.append(c);
        }

        fields.add(currField.toString());

        return fields;
    }
}