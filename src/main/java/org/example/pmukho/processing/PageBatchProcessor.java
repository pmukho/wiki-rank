package org.example.pmukho.processing;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
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

import it.unimi.dsi.fastutil.ints.IntSet;

public class PageBatchProcessor implements Consumer<Page>, AutoCloseable {
    private final IntSet pageIds;
    private final FileChannel out;
    private final int batchSize;
    private final BlockingQueue<Page> queue;
    private static final Page POISON = new Page(-1, 0, "<POISON>", false);
    private final ExecutorService executor;
    private final ByteBuffer buffer;

    public PageBatchProcessor(IntSet pageIds, FileChannel out, int batchSize) {
        this.pageIds = pageIds;
        this.out = out;
        this.batchSize = batchSize;
        this.queue = new LinkedBlockingQueue<>();
        this.executor = Executors.newSingleThreadExecutor();
        this.buffer = ByteBuffer.allocate(4*1024*1024);

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
            List<Page> batch = new ArrayList<>();

            while (true) {
                Page p = queue.take();

                if (p == PageBatchProcessor.POISON) {
                    if (!batch.isEmpty()) {
                        writeBatch(batch);
                    }
                    break;
                }

                batch.add(p);
                queue.drainTo(batch, batchSize);
                writeBatch(batch);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void writeBatch(List<Page> batch) throws IOException {
        /*
         * TODO:
         * write id, title pairs to output channel
         */
        for (Page p : batch) {
            int id = p.id();
            pageIds.add(id);

            byte[] title = p.title().getBytes(StandardCharsets.UTF_8);
            int needed = 4 + 4 + title.length;
            if (needed > buffer.remaining()) {
                flushBuffer();
            }

            buffer.putInt(id);
            buffer.putInt(title.length);
            buffer.put(title);
        }
        batch.clear();
    }

    private void flushBuffer() throws IOException {
        buffer.flip();
        while (buffer.hasRemaining()) {
            out.write(buffer);
        }
        buffer.clear();
    }

    @Override
    public void close() throws InterruptedException {
        queue.put(PageBatchProcessor.POISON);
        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.MINUTES);
    }

}
