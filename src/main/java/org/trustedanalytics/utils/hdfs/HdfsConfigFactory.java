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

import com.google.common.base.Strings;
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
import org.trustedanalytics.hadoop.config.ConfigurationHelperImpl;
import org.trustedanalytics.hadoop.config.ConfigurationLocator;
import org.trustedanalytics.hadoop.config.PropertyLocator;

import javax.security.auth.login.LoginException;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Map;


@org.springframework.context.annotation.Configuration
public class HdfsConfigFactory {

    private final ConfigurationHelper confHelper;

    private final KerberosHelper kerberosHelper;

    @Autowired
    private Environment env;

    @Value("${hdfs.user:}")
    private String hdfsUser;

    @Value("${hdfs.uri:}")
    private String hdfsUri;

    @Value("${kerberos.user:}")
    private String kerberosUser;

    @Value("${kerberos.pass:}")
    private String kerberosPass;

    public HdfsConfigFactory() {
        confHelper = ConfigurationHelperImpl.getInstance();
        kerberosHelper = new KerberosHelper(confHelper);
    }

    @Bean
    @Profile("cloud")
    public HdfsConfig configFromBroker() throws Exception {
        hdfsUri = getHdfsUriFromConfig();
        return createConfig(getConfigFromCf());
    }

    private Configuration getConfigFromCf() throws IOException {
        Configuration hadoopConfig = new Configuration(true);

        Map<String, String> config =
            confHelper.getConfigurationFromEnv(ConfigurationLocator.HADOOP);

        config.forEach(hadoopConfig::set);
        return hadoopConfig;
    }

    @Bean
    @Profile("hdfs-local")
    public HdfsConfig configFromEnv() throws Exception {
        return createConfig(new Configuration());
    }

    @Bean
    @Profile({"localfs", "default"})
    public HdfsConfig configOnLocalFS() throws IOException, LoginException {
        String folder = env.getProperty("FOLDER");
        if (Strings.isNullOrEmpty(folder)) {
            File tmpFolder = Files.createTempDir();
            tmpFolder.deleteOnExit();
            folder = tmpFolder.getAbsolutePath();
        }
        Configuration config = new Configuration(false);
        config.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, folder);
        FileSystem fileSystem = new MiniDFSCluster.Builder(config)
            .build()
            .getFileSystem();
        return createConfig(fileSystem, folder, "hdfs", config);
    }

    private String getHdfsUriFromConfig() throws Exception {
        return confHelper.getPropertyFromEnv(PropertyLocator.HDFS_URI)
            .orElseThrow(() -> new IllegalStateException("HDFS_URI not found in VCAP_SERVICES"));
    }

    private HdfsConfig createConfig(Configuration config) throws Exception {
        FileSystem fs = FileSystem.get(new URI(hdfsUri), config, hdfsUser);
        return createConfig(fs, hdfsUri, hdfsUser, config);
    }

    private HdfsConfig createConfig(FileSystem fileSystem, String hdfsUri, String hdfsUser,
        Configuration config) throws IOException, LoginException {

        if (kerberosHelper.isClusterIsSecuredByKerberos(config)) {
            kerberosHelper.login(config, kerberosUser, kerberosPass);
        }

        Path folder = new Path(hdfsUri);
        if (!fileSystem.exists(folder)) {
            fileSystem.mkdirs(folder);
        }
        fileSystem.setWorkingDirectory(folder);
        return new HdfsConfig(fileSystem, hdfsUser, folder);
    }


}
