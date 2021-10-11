package com.lordjoe.spark;

import java.io.Serializable;

/**
 * com.lordjoe.spark.NShot
 * User: Steve
 * Date: 3/28/2016
 */
public class NShot implements Serializable {

    public final int level;

    public NShot(int level) {
        this.level = level;
    }
    public NShot(NShot base) {
        this.level = base.level + 1;
    }
}
