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
import java.util.Map;


@org.springframework.context.annotation.Configuration
public class HdfsConfigFactory {

    private static String HADOOP_PARAMS_ENVVAR = "HADOOP_PARAMS";

    public enum Profiles {
        CLOUD, EMBEDDED
    }

    private final ConfigurationHelper confHelper;

    private final KerberosHelper kerberosHelper;

    @Autowired
    private Environment env;

    @Value("${hdfs.user:}")
    private String hdfsUser;

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
        return createConfig(getConfigFromCf(), getHdfsUriFromConfig());
    }

    private Configuration getConfigFromCf() throws IOException {
        Configuration hadoopConfig = new Configuration(true);

        Map<String, String> config =
            confHelper.getConfigurationFromEnv(ConfigurationLocator.HADOOP);

        config.forEach(hadoopConfig::set);
        return hadoopConfig;
    }

    @Bean
    @Profile("external")
    public HdfsConfig configFromEnv(@Value("${hdfs.uri:}") String hdfsUri) throws Exception {
        return createConfig(getConfigFromEnv(), hdfsUri);
    }

    @Bean
    @Profile("local")
    public HdfsConfig configLocalFS(@Value("${hdfs.uri:}") String hdfsUri) throws Exception {
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.getLocal(configuration);
        return createConfig(fileSystem, createTmpDir(), "hdfs", configuration);
    }

    private Configuration getConfigFromEnv() throws IOException {
        Configuration configuration = new Configuration();
        Map<String, String> map = confHelper.getConfigurationFromEnv(HADOOP_PARAMS_ENVVAR,
                ConfigurationLocator.HADOOP);
        map.forEach(configuration::set);
        return configuration;
    }


    private String createTmpDir() {
            File tmpFolder = Files.createTempDir();
            tmpFolder.deleteOnExit();
            return tmpFolder.getAbsolutePath();
    }

    private String getHdfsUriFromConfig() throws Exception {
        return confHelper.getPropertyFromEnv(PropertyLocator.HDFS_URI)
            .orElseThrow(() -> new IllegalStateException("HDFS_URI not found in VCAP_SERVICES"));
    }

    private HdfsConfig createConfig(Configuration config, String hdfsUri) throws Exception {
        loginIfNeeded(config);
        FileSystem fs = HdfsConfiguration.newInstance(config, hdfsUri, hdfsUser).getFileSystem();
        return createConfig(fs, hdfsUri, hdfsUser, config);
    }

    private HdfsConfig createConfig(FileSystem fileSystem, String hdfsUri, String hdfsUser,
        Configuration config) throws IOException, LoginException {
        Path folder = new Path(hdfsUri);
        fileSystem.setWorkingDirectory(folder);
        return new HdfsConfig(fileSystem, hdfsUser, folder);
    }

    /**
     * @param config
     * @throws IOException
     * @throws LoginException
     */
    private void loginIfNeeded(Configuration config) throws IOException, LoginException {
        if (kerberosHelper.isClusterIsSecuredByKerberos(config)) {
            kerberosHelper.login(config, kerberosUser, kerberosPass);
        }
    }
}
