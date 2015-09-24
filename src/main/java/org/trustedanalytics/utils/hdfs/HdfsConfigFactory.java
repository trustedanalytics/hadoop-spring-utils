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
import com.google.common.base.Strings;
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
import java.net.URI;
import java.util.Map;


@org.springframework.context.annotation.Configuration
public class HdfsConfigFactory {

    private static String HADOOP_PARAMS_ENVVAR = "HADOOP_PARAMS";

    private final ConfigurationHelper confHelper;

    private final KerberosHelper kerberosHelper;

    @Autowired
    private Environment env;

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
    @Profile("local")
    public HdfsConfig configLocalFS() throws Exception {
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.getLocal(configuration);
        String folder = env.getProperty("FOLDER");
        if (Strings.isNullOrEmpty(folder)) {
            return createConfig(fileSystem, createTmpDir(), "hdfs");
        }
        return createConfig(fileSystem, folder, "hdfs");
    }

    private String createTmpDir() {
            File tmpFolder = Files.createTempDir();
            tmpFolder.deleteOnExit();
            return tmpFolder.getAbsolutePath();
    }

    private String getHdfsUriFromConfig() throws Exception {
        return getPropertyFromCredentials(PropertyLocator.HDFS_URI);
    }

    private HdfsConfig createConfig(Configuration config, String hdfsUri) throws Exception {
        loginIfNeeded(config);
        FileSystem fs =
            FileSystem.get(new URI(hdfsUri), config,
                           getPropertyFromCredentials(PropertyLocator.USER));
        return createConfig(fs, hdfsUri, getPropertyFromCredentials(PropertyLocator.USER));
    }

    private HdfsConfig createConfig(FileSystem fileSystem, String hdfsUri, String hdfsUser)
        throws IOException, LoginException {
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
            kerberosHelper.login(config,
                                 getPropertyFromCredentials(PropertyLocator.USER),
                                 getPropertyFromCredentials(PropertyLocator.PASSWORD));
        }
    }

    private String getPropertyFromCredentials(PropertyLocator property) throws IOException{
        return confHelper.getPropertyFromEnv(property)
            .orElseThrow(() -> new IllegalStateException(
                property.name() + " not found in VCAP_SERVICES"));
    }
}
