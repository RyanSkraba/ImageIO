package com.skraba.byexample.beam.io;

import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;

import static org.hamcrest.io.FileMatchers.anExistingFile;
import static org.junit.Assert.assertThat;

/**
 * Unit tests for {@link ImageIO}.
 * <p>
 * TODO: These make HTTP callouts to the web.
 */
public class ImageIOTest {

    public static final String SMALL_PIKACHU = "https://66.media.tumblr.com/60aeee62dc1aee0c3c0fbad1702eb860/tumblr_inline_pfp352ORsk1r4hkfd_250.png";
    public static final String CGI_PIKACHU = "https://en.meming.world/images/en/4/44/Surprised_Pikachu_Detective.jpg";
    public static final String HD_PIKACHU = "https://en.meming.world/images/en/thumb/2/2c/Surprised_Pikachu_HD.jpg/621px-Surprised_Pikachu_HD.jpg";
    public static final String RENDERED_PIKACHU = "https://en.meming.world/images/en/a/af/Surprised_Pikachu_3D.jpg";

    @Rule
    public TestPipeline p = TestPipeline.create().enableAbandonedNodeEnforcement(false);

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void read() throws MalformedURLException {
        PCollection<Pixel> in = p.apply(ImageIO.read(new URL(SMALL_PIKACHU), false));
        PAssert.that(in.apply(Count.globally())).containsInAnyOrder(43485L);
        PipelineResult result = p.run();
    }

    @Test
    public void readWithSample() throws MalformedURLException {
        PCollection<Pixel> in = p.apply(ImageIO.readWithSample(new URL(HD_PIKACHU), 10));
        PAssert.that(in).satisfies(
                (SerializableFunction<Iterable<Pixel>, Void>) input -> {
                    for (Pixel px : input) {
                        System.out.println(px);
                    }
                    return null;
                }
        );
        PAssert.that(in.apply(Count.globally())).containsInAnyOrder(10L);
        PipelineResult result = p.run();
    }

    @Test
    public void write() throws IOException {
        File out = folder.newFile();
        PCollection<Pixel> in = p.apply(ImageIO.read(new URL(SMALL_PIKACHU), false));
        in.apply(ImageIO.write(out.getAbsolutePath(), 1));
        PAssert.that(in.apply(Count.globally())).containsInAnyOrder(43485L);
        p.run();

        assertThat(out, anExistingFile());
    }
}