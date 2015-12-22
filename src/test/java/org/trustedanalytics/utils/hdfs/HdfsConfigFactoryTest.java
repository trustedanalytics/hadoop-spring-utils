/**
 * Copyright (c) 2015 Intel Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.trustedanalytics.utils.hdfs;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ActiveProfiles("embedded")
@ContextConfiguration(classes = TestHdfsConfigFactory.class)
public class HdfsConfigFactoryTest {

    private static final String SOME_FILE_NAME = "some_file";
    private static final String APPEND_TEXT = "test-append";

    @Autowired
    private HdfsConfig hdfsConfig;

    @Test
    public void verifyLocalFileSystemInTmpFolder() throws IOException {
        FileSystem fs = hdfsConfig.getFileSystem();
        assertThat("FileSystem wasn't created",
                fs, is(notNullValue()));
        verifyFileSystem(fs);
    }

    private void verifyFileSystem(FileSystem fs) throws IOException {
        fs.createNewFile(new Path(SOME_FILE_NAME));
        RemoteIterator<LocatedFileStatus> files = fs.listFiles(hdfsConfig.getPath(), false);
        assertThat("Creating file failed",
                files.hasNext(), equalTo(true));
        LocatedFileStatus remoteFile = files.next();
        assertThat("file wasn't created in proper folder",
                remoteFile.getPath().toUri().toString(),
                containsString(hdfsConfig.getPath().toUri().toString()));
        assertThat("file wasn't created in proper folder",
                remoteFile.getPath().toUri().toString(),
                containsString(SOME_FILE_NAME));
        assertThat("More then 1 file was created",
                files.hasNext(), equalTo(false));
        testAppendFunction(fs, remoteFile);
    }

    private void testAppendFunction(FileSystem fs, LocatedFileStatus remoteFile) throws IOException {
        try (OutputStream out = fs.append(remoteFile.getPath())) {
            out.write(APPEND_TEXT.getBytes());
        }

        try (InputStreamReader in = new InputStreamReader(fs.open(remoteFile.getPath()));
                BufferedReader br = new BufferedReader(in)) {
            String content = br.readLine();
            assert br.readLine() == null;
            assertThat("File was not appended properly", content, containsString(APPEND_TEXT));
        }
    }
}
