package org.example.pmukho;

import java.io.IOException;

import org.example.pmukho.core.*;

public class Main {
    public static void main(String[] args) {

        String gzipFile = "./data/page.sql.gz";

        SqlStreamReader sqlReader = new SqlStreamReader();
        sqlReader.read(gzipFile);
    }
}
