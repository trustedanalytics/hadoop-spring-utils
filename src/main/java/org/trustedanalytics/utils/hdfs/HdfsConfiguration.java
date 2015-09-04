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

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.login.LoginException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HdfsConfiguration {

    private static final Logger LOGGER = LoggerFactory.getLogger(HdfsConfiguration.class);

    private static final String AUTHENTICATION_METHOD_NAME = "kerberos";

    private Configuration hadoopConf;

    private String userName;

    private String hdfsUri;

    public static HdfsConfiguration newInstance(Configuration hadoopConf, String hdfsUri, String userName) {
        return new HdfsConfiguration(hadoopConf, hdfsUri, userName);
    }
    private HdfsConfiguration(Configuration hadoopConf, String hdfsUri, String userName) {
        Preconditions.checkNotNull(hadoopConf, "Hadoop configuration cannot be null");
        this.hadoopConf = hadoopConf;
        this.hdfsUri = hdfsUri;
        this.userName = userName;
    }

    public FileSystem getFileSystem() throws InterruptedException,
            URISyntaxException, LoginException, IOException {

        if (AUTHENTICATION_METHOD_NAME.equals(hadoopConf.get(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION))) {
            return getSecureFileSystem();
        } else {
            return getInsecureFileSystem();
        }
    }

    private FileSystem getSecureFileSystem() throws InterruptedException,
            URISyntaxException, LoginException, IOException {
        return FileSystem.get(hadoopConf);
    }

    private FileSystem getInsecureFileSystem() throws InterruptedException,
            URISyntaxException, LoginException, IOException {
        Preconditions.checkNotNull(userName, "Insecure file system needs valid username");
        return FileSystem.get(new URI(hdfsUri), hadoopConf, userName);
    }

}
