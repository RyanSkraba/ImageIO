package com.skraba.byexample.beam.io;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.joda.time.Instant;

import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.net.URL;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Random;

/**
 * Reads {@link Pixel} records from an URL.
 */
public class PixelSource extends BoundedSource<Pixel> {

    private final URL url;

    private final int limit;

    public PixelSource(URL url) {
        this.url = url;
        this.limit = -1;
    }

    public PixelSource(URL url, int limit) {
        this.url = url;
        this.limit = limit;
    }

    @Override
    public List<? extends BoundedSource<Pixel>> split(long desiredBundleSizeBytes, PipelineOptions options) throws Exception {
        return Collections.singletonList(this);
    }

    @Override
    public long getEstimatedSizeBytes(PipelineOptions options) throws Exception {
        return 0;
    }

    @Override
    public PixelReader createReader(PipelineOptions options) throws IOException {
        return limit == -1 ? new PixelReader(this) : new PixelSampler(this);
    }

    @Override
    public Coder<Pixel> getOutputCoder() {
        return AvroCoder.of(Pixel.class);
    }

    public URL getUrl() {
        return this.url;
    }

    public int getLimit() {
        return limit;
    }

    public static class PixelReader extends BoundedSource.BoundedReader<Pixel> {

        private final PixelSource src;

        BufferedImage img;
        Pixel p;
        int width = 0;
        int height = 0;
        private int x = -1;
        private int y = 0;

        public PixelReader(PixelSource src) {
            this.src = src;
        }

        @Override
        public boolean start() throws IOException {
            img = javax.imageio.ImageIO.read(src.getUrl());
            width = img.getWidth();
            height = img.getHeight();
            return advance();
        }

        @Override
        public boolean advance() throws IOException {
            x += 1;
            if (x >= width) {
                x = 0;
                y += 1;
            }
            if (y < height) {
                Color c = new Color(img.getRGB(x, y), true);
                p = new Pixel(width, height, x, y, c.getRed(), c.getGreen(), c.getBlue(), c.getAlpha());
                return true;
            }
            return false;
        }

        @Override
        public Pixel getCurrent() throws NoSuchElementException {
            if (p == null)
                throw new NoSuchElementException();
            return p;
        }

        @Override
        public Instant getCurrentTimestamp() throws NoSuchElementException {
            if (p == null)
                throw new NoSuchElementException();
            return BoundedWindow.TIMESTAMP_MIN_VALUE;
        }

        @Override
        public void close() throws IOException {

        }

        @Override
        public PixelSource getCurrentSource() {
            return src;
        }
    }

    public static class PixelSampler extends PixelReader {

        int count = 0;
        private Random rnd = new Random();

        public PixelSampler(PixelSource src) {
            super(src);
        }


        @Override
        public boolean advance() throws IOException {
            int x = rnd.nextInt(width);
            int y = rnd.nextInt(height);
            Color c = new Color(img.getRGB(x, y), true);
            p = new Pixel(width, height, x, y, c.getRed(), c.getGreen(), c.getBlue(), c.getAlpha());
            count++;
            return count <= getCurrentSource().getLimit();
        }

    }
}