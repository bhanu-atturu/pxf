package org.greenplum.pxf.plugins.json.parser;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class PartitionedJsonParserTest {

    @Test
    public void testOffset() throws IOException, URISyntaxException {
       File file = new File(this.getClass().getClassLoader().getResource("parser-tests/offset/input.json").toURI());

        InputStream jsonInputStream = new FileInputStream(file);

        PartitionedJsonParser parser = new PartitionedJsonParser(jsonInputStream);
        String result = parser.nextObjectContainingMember("cüsötmerstätüs");
        assertNotNull(result);
        assertEquals(107, parser.getBytesRead());
        assertEquals(11, parser.getBytesRead() - result.length());

        result = parser.nextObjectContainingMember("cüsötmerstätüs");
        assertNotNull(result);
        assertEquals(216, parser.getBytesRead());
        assertEquals(116, parser.getBytesRead() - result.length());
        jsonInputStream.close();
    }

}