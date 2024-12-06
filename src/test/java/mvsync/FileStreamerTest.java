package mvsync;

import mvsync.output.FileStreamer;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;

public class FileStreamerTest {
    String path = "./test.txt";

    @Test
    public void testAppend() throws Exception {

        // create a new file
        FileStreamer fileStreamer = new FileStreamer(path);
        fileStreamer.append("Hello");
        assertEquals("Hello"+ System.lineSeparator(), Files.readString(Paths.get(path)));

        // file already exists - delete and recreate
        FileStreamer fileStreamer1 = new FileStreamer(path);
        fileStreamer1.append("World");
        assertEquals("World"+ System.lineSeparator(), Files.readString(Paths.get(path)));
    }

    @Test
    public void testCommit() throws IOException {
        FileStreamer fileStreamer = new FileStreamer(path);
        fileStreamer.commit();
    }
}
