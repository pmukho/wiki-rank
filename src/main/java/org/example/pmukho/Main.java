package org.example.pmukho;

import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.function.Function;

import org.example.pmukho.parser.*;
import org.example.pmukho.processing.PageBatchProcessor;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;

public class Main {

    public static void main(String[] args) {
        Path pageDumpFile = Paths.get("./data/page.sql.gz");
        Function<List<String>, Page> pageBuilder = fields -> new Page(
                Integer.parseInt(fields.get(0)),
                Integer.parseInt(fields.get(1)),
                fields.get(2),
                Boolean.parseBoolean(fields.get(3)));

        System.out.println("STARTING PAGE DUMP PROCESSING");
        /*
         * According to https://en.wikipedia.org/wiki/Wikipedia:Size_of_Wikipedia,
         * There are roughly ~7 million articles (namespace=0 pages)
         */
        IntSet pageIds = new IntOpenHashSet(10_000_000);
        Path idTitlePath = Paths.get("./data/title-lookup.bin");
        try (FileChannel out = FileChannel.open(idTitlePath,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING);
                PageBatchProcessor pageConsumer = new PageBatchProcessor(pageIds, out, 1000)) {

            SqlStreamReader.read(pageDumpFile, "page", pageBuilder, pageConsumer);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("FINISHING PAGE DUMP PROCESSING");
        System.out.println(pageIds.size());

        /*
         * TODO:
         * parse pagelinks dump
         */
        // Path linksDumpPath = Paths.get("./data/pagelinks.sql.gz");
        // Function<List<String>, Link> linkBuilder = fields -> new Link(
        //         Integer.parseInt(fields.get(0)),
        //         Integer.parseInt(fields.get(1)),
        //         Integer.parseInt(fields.get(2)));
    }
}
