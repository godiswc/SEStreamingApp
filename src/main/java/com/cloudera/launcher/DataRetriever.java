package com.cloudera.launcher;

import java.io.IOException;

/**
 * Created by root on 5/31/17.
 */
public interface DataRetriever<A,B> {
    A retrieve();
   // A retrieve(long frequency);
    A retrieve(long frequency, B b) throws IOException;

}
