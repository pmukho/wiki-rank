package org.example.pmukho;

import java.io.IOException;

import org.example.pmukho.core.*;

public class Main {
    public static void main(String[] args) {

        String gzipFile = "./data/page.sql.gz";

        SqlStreamReader ssr = new SqlStreamReader();
        try {
            ssr.read(gzipFile);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
