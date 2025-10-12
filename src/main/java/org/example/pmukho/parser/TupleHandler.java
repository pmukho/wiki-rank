package org.example.pmukho.parser;

import java.util.List;

@FunctionalInterface
public interface TupleHandler {
    void onTuple(List<String> fields);
}