package org.example.pmukho.parser;

/*
 * See link below for info on fields
 * https://www.mediawiki.org/wiki/Manual:Pagelinks_table
 */

public record Link(
        int from,
        int target,
        int namespace) {
}
