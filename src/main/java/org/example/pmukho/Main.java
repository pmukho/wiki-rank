package org.example.pmukho;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.example.pmukho.parser.*;

public class Main {
    public static void main(String[] args) {

        Path gzipFile = Paths.get("./data/page.sql.gz");

        SqlStreamReader sqlReader = new SqlStreamReader();
        RowCounter rc = new RowCounter();
        sqlReader.read(gzipFile, "page", tuple -> rc.inc(tuple));

        System.out.printf("Total count: %d, Article count: %d\n", rc.totalCount, rc.articleCount);
        System.out.println("Tuple sizes: " + rc.tupleSizes);
    }

    static class RowCounter {
        long totalCount = 0;
        long articleCount = 0;
        Map<Integer, Long> tupleSizes = new HashMap<>();

        void inc(List<String> tuple) {
            totalCount++;
            if (tuple.get(1).equals("0"))
                articleCount++;

            int numFields = tuple.size();
            tupleSizes.put(numFields, tupleSizes.getOrDefault(numFields, (long) 0) + 1);
        }
    }
}
