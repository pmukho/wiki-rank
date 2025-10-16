package org.example.pmukho;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.example.pmukho.parser.*;

public class Main {
    public static void main(String[] args) {

        Path gzipFile = Paths.get("./data/pagelinks.sql.gz");

        SqlStreamReader sqlReader = new SqlStreamReader();
        RowCounter rc = new RowCounter();
        sqlReader.read(gzipFile, "pagelinks", tuple -> rc.inc(tuple));

        System.out.println("Total count: " + rc.count);
        System.out.println("Tuple sizes: " + rc.tupleSizes);
    }

    static class RowCounter {
        long count = 0;
        Map<Integer, Long> tupleSizes = new HashMap<>();

        void inc(List<String> tuple) {
            count++;

            int numFields = tuple.size();
            tupleSizes.put(numFields, tupleSizes.getOrDefault(numFields, (long) 0) + 1);
        }
    }
}
