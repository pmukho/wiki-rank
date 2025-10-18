package org.example.pmukho.parser;

/*
 * See link below for info on fields
 * https://www.mediawiki.org/wiki/Manual:Page_table
 */

public record Page(
        int id,
        int namespace,
        String title,
        boolean isRedirect) {
}
