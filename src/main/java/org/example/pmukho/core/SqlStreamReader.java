package org.example.pmukho.core;

import java.io.*;
import java.util.zip.GZIPInputStream;

public class SqlStreamReader {

    public void read(String path) throws IOException {
        InputStream fileStream = new FileInputStream(path);
        InputStream in = new GZIPInputStream(fileStream);
        BufferedReader br = new BufferedReader(new InputStreamReader(in));

        String line;
        while ((line = br.readLine()) != null) {
            if (!line.startsWith("INSERT INTO"))
                continue;

            /*
             * TODO:
             * - create parser that returns tuple as list of String
             * - have downstream task be responsible for casting fields
             */

        }

        System.out.println("DONE READING");
    }
}