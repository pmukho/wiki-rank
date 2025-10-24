package org.example.pmukho.processing;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.example.pmukho.parser.Page;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

public class PageBatchProcessor implements Consumer<Page>, AutoCloseable {
    private final RocksDB db;
    private final int batchSize;
    private final BlockingQueue<Page> queue;
    private static final Page POISON = new Page(-1, -1, "<POISON>", false);
    private final ExecutorService executor;

    static {
        RocksDB.loadLibrary();
    }

    public PageBatchProcessor(RocksDB db, int batchSize) {
        this.db = db;
        this.batchSize = batchSize;
        this.queue = new LinkedBlockingQueue<>();
        this.executor = Executors.newSingleThreadExecutor();

        executor.submit(this::loopBatch);
    }

    @Override
    public void accept(Page page) {
        if (page.namespace() != 0 || page.isRedirect())
            return;

        try {
            queue.put(page);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void loopBatch() {
        try {
            while (true) {
                List<Page> batch = new ArrayList<>();
                Page p = queue.take();

                if (p == PageBatchProcessor.POISON)
                    break;

                batch.add(p);
                queue.drainTo(batch, batchSize);

                writeBatchToDB(batch);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    private void writeBatchToDB(List<Page> batch) {
        try (WriteBatch wb = new WriteBatch();
                WriteOptions writeOptions = new WriteOptions()) {
            for (Page p : batch) {
                int id = p.id();
                byte[] key = new byte[4];
                key[0] = (byte) ((id >> 24) & 0xFF);
                key[1] = (byte) ((id >> 16) & 0xFF);
                key[2] = (byte) ((id >> 8) & 0xFF);
                key[3] = (byte) (id & 0xFF);

                byte[] value = p.title().getBytes(StandardCharsets.UTF_8);

                wb.put(key, value);
            }

            db.write(writeOptions, wb);
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() throws InterruptedException {
        queue.put(PageBatchProcessor.POISON);
        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.MINUTES);
    }

}
