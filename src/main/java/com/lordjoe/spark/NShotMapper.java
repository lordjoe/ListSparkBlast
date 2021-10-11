package com.lordjoe.spark;

import org.apache.spark.api.java.function.FlatMapFunction;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * com.lordjoe.spark.NShotMapper
 * use a flatmap to blow up an NShot by   expansionFactor
 * User: Steve
 * Date: 3/28/2016
 */
public class NShotMapper implements FlatMapFunction<NShot,NShot> ,Serializable {
    public NShotMapper(int expansionFactor) {
        this.expansionFactor = expansionFactor;
    }

    public final int expansionFactor;

    @Override
    public Iterable<NShot> call(NShot nShot) throws Exception {
        List<NShot> holder = new ArrayList<NShot>();
        for (int i = 0; i < expansionFactor; i++) {
           holder.add(new NShot(nShot ));
          }
         return holder;
     }
}
