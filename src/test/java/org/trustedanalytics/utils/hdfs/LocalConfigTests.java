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

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.hamcrest.Matcher;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

@RunWith(SpringJUnit4ClassRunner.class)
@ActiveProfiles("local")
@Configuration
@ContextConfiguration(classes = LocalConfigTests.class)
@EnableHdfs
public class LocalConfigTests {

    private static final String FOLDER_PROPERTY = "FOLDER";
    private static java.nio.file.Path DESTINATION_PATH;

    @BeforeClass
    public static void initializeTmpFolder() throws IOException {
        // we use relative path as it seems to be harder case
        DESTINATION_PATH = Paths.get("local_hdfs_tests_folder_" + randomString());
        System.setProperty(FOLDER_PROPERTY, DESTINATION_PATH.toString());
    }

    @AfterClass
    public static void cleanupTmpFolder() throws IOException {
        FileUtils.deleteDirectory(DESTINATION_PATH.toFile());
    }

    private static String randomString() {
        return UUID.randomUUID().toString();
    }

    @Autowired
    private HdfsConfig hdfsConfig;

    @Test
    public void testFolderParameterUsage() throws IOException {

        Path workingDir = hdfsConfig.getFileSystem().getWorkingDirectory();
        assertPathsEqual("HDFS working folder is different then specified",
                workingDir, DESTINATION_PATH.toAbsolutePath());

        String randomFilename = randomString();
        Path hdfsPath = new Path(hdfsConfig.getPath(), randomFilename);
        java.nio.file.Path localPath = Paths.get(DESTINATION_PATH.toString(), randomFilename);
        assertPathsEqual("Paths doesn't match while using path parameter from HdfsConfig",
                hdfsPath, localPath.toAbsolutePath());
    }

    private void assertPathsEqual(String errorMsg, Path hdfsPath, java.nio.file.Path localPath) {
        String prefixFreePath = hdfsConfig.getFileSystem()
                .makeQualified(hdfsPath)
                .toString()
                .substring(5); // remove file: prefix
        assertThat(errorMsg, localPath.toString(), equalTo(prefixFreePath));
    }
}
