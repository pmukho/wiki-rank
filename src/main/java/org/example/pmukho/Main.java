package org.example.pmukho;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.function.Function;

import org.example.pmukho.parser.*;
import org.example.pmukho.processing.PageBatchProcessor;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

public class Main {

    public static void main(String[] args) {
        RocksDB.loadLibrary();

        Path gzipFile = Paths.get("./data/page.sql.gz");
        Function<List<String>, Page> pageBuilder = fields -> new Page(
                Integer.parseInt(fields.get(0)),
                Integer.parseInt(fields.get(1)),
                fields.get(2),
                Boolean.parseBoolean(fields.get(3)));

        try (final Options options = new Options().setCreateIfMissing(true);
                RocksDB db = RocksDB.open(options, "./data/db");
                PageBatchProcessor consumer = new PageBatchProcessor(db, 500)) {

            SqlStreamReader<Page> parser = new SqlStreamReader<>();
            parser.read(gzipFile, "page", pageBuilder, consumer);
            System.out.println("DONE READING");

        } catch (RocksDBException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    static class RowCounter {
        long totalCount = 0;
        long articleCount = 0;

        void inc(int namespace) {
            totalCount++;
            if (namespace == 0)
                articleCount++;
        }
    }
}
