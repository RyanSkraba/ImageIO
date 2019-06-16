package com.skraba.byexample.beam.io;

import org.apache.avro.generic.IndexedRecord;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;

/**
 * Writes {@link Pixel} data to an image.
 *
 * TODO: Use the Beam File base classes.
 */
public class PixelSink extends PTransform<PCollection<? extends IndexedRecord>, PDone> {

    private final String filename;

    private final int shards;

    public PixelSink(String filename, int shards) {
        this.filename = filename;
        this.shards = shards;
    }

    @Override
    public PDone expand(PCollection<? extends IndexedRecord> input) {
        input
                .apply(
                        "ShardToOneFile",
                        MapElements.via(
                                new SimpleFunction<IndexedRecord, KV<Void, IndexedRecord>>() {
                                    @Override
                                    public KV<Void, IndexedRecord> apply(IndexedRecord element) {
                                        return KV.of((Void) null, element);
                                    }
                                }))
                .apply(GroupByKey.create())
                .apply(Values.create())
                .apply(ParDo.of(new PixelWriter(filename)));
        return PDone.in(input.getPipeline());
    }

    private class PixelWriter extends DoFn<Iterable<IndexedRecord>, Void> {
        private final String filename;

        public PixelWriter(String filename) {
            this.filename = filename;
        }

        BufferedImage img;

        /**
         * Pass through the data without modification.
         */
        @ProcessElement
        public void processElement(ProcessContext c) {
            for (IndexedRecord r : c.element()) {
                if (img == null) {
                    img = new BufferedImage((int) r.get(0), (int) r.get(1), BufferedImage.TYPE_INT_ARGB);
                }
                img.setRGB((int) r.get(2), (int) r.get(3), (int) r.get(7) << 24 | (int) r.get(4) << 16 | (int) r.get(5) << 8 | (int) r.get(6));
            }
        }

        @FinishBundle
        public void finish() throws IOException {
            File file = new File(shards == 1 ? filename : filename);
            javax.imageio.ImageIO.write(img, "png", file);
        }

    }
}
