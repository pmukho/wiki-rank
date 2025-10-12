package org.example.pmukho;

import java.nio.file.Path;
import java.nio.file.Paths;

import org.example.pmukho.parser.*;

public class Main {
    public static void main(String[] args) {

        Path gzipFile = Paths.get("./data/page.sql.gz");

        SqlStreamReader sqlReader = new SqlStreamReader();
        RowCounter rc = new RowCounter();
        sqlReader.read(gzipFile, tuple -> rc.inc());

        System.out.printf("Total count: %d\n", rc.count);
    }

    static class RowCounter {
        long count = 0;

        void inc() {
            count++;
            if (count % 1e6 == 0)
                System.out.println("So far: " + count);
        }
    }
}
