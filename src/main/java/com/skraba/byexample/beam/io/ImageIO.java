package com.skraba.byexample.beam.io;

import org.apache.avro.generic.IndexedRecord;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

import java.net.URL;

/**
 * A simple Beam IO reading pixels from an image, and writing pixels to an image.
 */
public class ImageIO {

    public static PTransform<PBegin, PCollection<Pixel>> read(URL url, boolean reshuffle) {
        return new PixelRead(url, reshuffle);
    }

    public static PTransform<PBegin, PCollection<Pixel>> readWithSample(URL url, int limit) {
        return new PixelSample(url, limit);
    }

    public static PTransform<PCollection<? extends IndexedRecord>, PDone> write(String filename, int shards) {
        return new PixelSink(filename, shards);
    }

    public static class PixelRead extends PTransform<PBegin, PCollection<Pixel>> {

        private final URL url;

        private final boolean reshuffle;

        public PixelRead(URL url, boolean reshuffle) {
            this.url = url;
            this.reshuffle = reshuffle;
        }

        @Override
        public PCollection<Pixel> expand(PBegin begin) {
            PCollection<Pixel> in = begin.apply(Read.from(new PixelSource(url)));
            return reshuffle ? in.apply(Reshuffle.viaRandomKey()) : in;
        }
    }

    public static class PixelSample extends PTransform<PBegin, PCollection<Pixel>> {

        private final URL url;
        private final int limit;

        public PixelSample(URL url, int limit) {
            this.url = url;
            this.limit = limit;
        }

        @Override
        public PCollection<Pixel> expand(PBegin input) {
            return input.apply(Read.from(new PixelSource(url, limit)));
        }
    }

}