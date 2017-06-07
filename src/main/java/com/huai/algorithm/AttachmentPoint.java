package com.huai.algorithm;

import org.apache.commons.math3.ml.clustering.Clusterable;

/**
 * Created by liangyh on 2/21/17.
 */
public class AttachmentPoint<T> implements Clusterable {

    T attachment = null;

    double longitude = 0.0D;

    double latitude = 0.0D;

    public AttachmentPoint(double longitude, double latitude){
        this.longitude = longitude;
        this.latitude = latitude;
    }

    public AttachmentPoint(double longitude, double latitude, T attachment){
        this.longitude = longitude;
        this.latitude = latitude;
        this.attachment = attachment;
    }

    @Override
    public double[] getPoint() {
        return new double[]{longitude, latitude};
    }

    @Override
    public String toString() {
        return "AttachmentPoint{" +
                "attachment=" + attachment +
                ", longitude=" + longitude +
                ", latitude=" + latitude +
                '}';
    }
}
