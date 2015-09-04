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

import com.google.common.io.Files;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;
import org.springframework.core.env.Environment;
import org.trustedanalytics.hadoop.config.ConfigurationHelper;

import javax.security.auth.login.LoginException;
import java.io.File;
import java.io.IOException;


@org.springframework.context.annotation.Configuration
public class TestHdfsConfigFactory {

    private static String HADOOP_PARAMS_ENVVAR = "HADOOP_PARAMS";

    @Autowired
    private Environment env;

    @Value("${hdfs.user:}")
    private String hdfsUser;

    @Value("${kerberos.user:}")
    private String kerberosUser;

    @Value("${kerberos.pass:}")
    private String kerberosPass;


    @Bean
    @Profile("embedded")
    public HdfsConfig configEmbedded()
            throws IOException, LoginException {
        String tmpDir = createTmpDir();
        Configuration config = new Configuration(false);
        config.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, tmpDir);
        FileSystem fileSystem = new MiniDFSCluster.Builder(config)
                .build()
                .getFileSystem();

        return createConfig(fileSystem, tmpDir, "hdfs", config);
    }


    private String createTmpDir() {
        File tmpFolder = Files.createTempDir();
        tmpFolder.deleteOnExit();
        return tmpFolder.getAbsolutePath();
    }

    private HdfsConfig createConfig(FileSystem fileSystem, String hdfsUri, String hdfsUser,
                                    Configuration config) throws IOException, LoginException {
        Path folder = new Path(hdfsUri);
        fileSystem.setWorkingDirectory(folder);
        return new HdfsConfig(fileSystem, hdfsUser, folder);
    }

}
