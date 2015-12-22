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
import com.google.common.base.Strings;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.trustedanalytics.hadoop.config.ConfigurationHelper;
import org.trustedanalytics.hadoop.config.PropertyLocator;
import org.trustedanalytics.hadoop.kerberos.KrbLoginManager;
import org.trustedanalytics.hadoop.kerberos.KrbLoginManagerFactory;

import javax.security.auth.login.LoginException;
import java.io.IOException;

public class KerberosHelper {

    private static final Logger LOGGER = LogManager.getLogger(KerberosHelper.class);

    private static final String AUTHENTICATION_METHOD = "kerberos";

    private static final String AUTHENTICATION_METHOD_PROPERTY = "hadoop.security.authentication";

    private final ConfigurationHelper confHelper;

    public KerberosHelper(ConfigurationHelper confHelper) {
        this.confHelper = confHelper;
    }

    public boolean isClusterIsSecuredByKerberos(org.apache.hadoop.conf.Configuration config) {
        return AUTHENTICATION_METHOD.equals(config.get(AUTHENTICATION_METHOD_PROPERTY));
    }

    public void login(Configuration config, String kerberosUser, String kerberosPass)
        throws IOException, LoginException {

        String kdc = confHelper.getPropertyFromEnv(PropertyLocator.KRB_KDC)
            .orElseThrow(() -> new IllegalStateException("KRB_KDC not found in configuration"));
        String realm = confHelper.getPropertyFromEnv(PropertyLocator.KRB_REALM)
            .orElseThrow(() -> new IllegalStateException("KRB_REALM not found in configuration"));
        Preconditions.checkArgument(!Strings.isNullOrEmpty(kerberosUser),
            "KERBEROS_USER not found in configuration");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(kerberosPass),
            "KERBEROS_PASS not found in configuration");

        LOGGER.info("Setting kerberos kdc and realm : " + kdc + ", " + realm);
        KrbLoginManager loginManager = KrbLoginManagerFactory.getInstance().getKrbLoginManagerInstance(
                kdc, realm);

        loginManager.loginInHadoop(
            loginManager.loginWithCredentials(kerberosUser, kerberosPass.toCharArray()), config);
    }
}
