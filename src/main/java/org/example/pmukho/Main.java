package org.example.pmukho;

import java.nio.file.Path;
import java.nio.file.Paths;

import org.example.pmukho.parser.*;

public class Main {
    public static void main(String[] args) {

        Path gzipFile = Paths.get("./data/page.sql.gz");

        SqlStreamReader<Integer> sqlReader = new SqlStreamReader<>();
        RowCounter rc = new RowCounter();
        sqlReader.read(gzipFile, "page", row -> Integer.parseInt(row.get(1)), namespace -> rc.inc(namespace));

        System.out.printf("Total count: %d, Article count: %d\n", rc.totalCount, rc.articleCount);
    }

    static class RowCounter {
        long totalCount = 0;
        long articleCount = 0;

        void inc(int namespace) {
            totalCount++;
            if (namespace == 0) articleCount++;
        }
    }
}
