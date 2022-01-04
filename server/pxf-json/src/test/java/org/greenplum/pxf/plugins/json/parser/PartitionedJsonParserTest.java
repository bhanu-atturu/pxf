package org.greenplum.pxf.plugins.json.parser;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.*;

public class PartitionedJsonParserTest {

    @Test
    public void testUnicode() throws IOException {
        InputStream inputStream = getFileFromResourcesAsStream("test-unicode.json");
        PartitionedJsonParser jsonParser = new PartitionedJsonParser(inputStream);

        assertNotNull(jsonParser.nextObjectContainingMember("en"));
        assertEquals(45, jsonParser.getBytesRead());

        assertNotNull(jsonParser.nextObjectContainingMember("en"));
        assertEquals(99, jsonParser.getBytesRead());

        assertNotNull(jsonParser.nextObjectContainingMember("en"));
        assertEquals(147, jsonParser.getBytesRead());

        assertNull(jsonParser.nextObjectContainingMember("en"));
    }

    @Test
    public void testUnicodeEmoji() throws IOException {
        InputStream inputStream = getFileFromResourcesAsStream("test-emoji.json");
        PartitionedJsonParser jsonParser = new PartitionedJsonParser(inputStream);

        String nextObject = jsonParser.nextObjectContainingMember("a");
        assertEquals("{\n    \"a\": \"\uD83D\uDCA9\"\n  }", nextObject);
        assertEquals(25, jsonParser.getBytesRead());

        assertNull(jsonParser.nextObjectContainingMember("a"));
    }

    private InputStream getFileFromResourcesAsStream(String fileName) {
        ClassLoader classLoader = getClass().getClassLoader();
        InputStream resourceAsStream = classLoader.getResourceAsStream(Paths.get("parser-tests", fileName).toString());

        if (resourceAsStream == null) {
            throw new IllegalArgumentException("file '" + fileName + "' not found...");
        }

        return resourceAsStream;
    }
}
