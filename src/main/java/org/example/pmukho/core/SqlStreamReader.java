package org.example.pmukho.core;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

public class SqlStreamReader {

    public void read(String path) {

        try (InputStream fileStream = new FileInputStream(path);
                InputStream in = path.endsWith(".gz") ? new GZIPInputStream(fileStream) : fileStream;
                BufferedReader br = new BufferedReader(new InputStreamReader(in))) {

            String line;
            SqlRowParser parser = new SqlRowParser();
            // List<List<String>> rows = new ArrayList<>();
            while ((line = br.readLine()) != null) {
                if (!line.startsWith("INSERT INTO"))
                    continue;

                parser.parseInsert(line);
            }

            System.out.println("DONE READING");

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}