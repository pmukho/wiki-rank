package org.example.pmukho;

import java.io.*;
import java.util.zip.GZIPInputStream;

public class App {
    public static void main(String[] args) {

        String gzipFile = "./data/page.sql.gz";

        /*
         * See link to learn about sql format
         * https://meta.wikimedia.org/wiki/Data_dumps/Dump_format
         */
        try (FileInputStream fis = new FileInputStream(gzipFile);
                GZIPInputStream gis = new GZIPInputStream(fis);
                BufferedReader br = new BufferedReader(new InputStreamReader(gis))) {

            String line;
            outer: while ((line = br.readLine()) != null) {
                if (line.startsWith("INSERT INTO `page`")) {
                    int valuesStart = line.indexOf('(');
                    if (valuesStart < 0)
                        continue;

                    String[] rows = line.substring(valuesStart).split("\\),\\(");
                    int numRows = rows.length;
                    // need to trim initial '(' and ending')'
                    if (numRows > 0) {
                        rows[0] = rows[0].substring(1);
                        rows[numRows-1] = rows[numRows-1].substring(0, rows[numRows-1].length()-1);
                    }

                    for (String row : rows) {
                        String[] cols = row.split(",", 4);

                        int pageId = Integer.parseInt(cols[0]);
                        int namespace = Integer.parseInt(cols[1]);
                        String pageTitle = cols[2];

                        if (namespace == 0) {
                            System.out.printf("(%d) %s\n", pageId, pageTitle);
                            break outer;
                        }
                    }
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
