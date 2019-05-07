/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.repositories.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.AbstractFileSystem;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.io.retry.FailoverProxyProvider;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.List;
import java.util.Locale;

public final class HdfsRepository extends BlobStoreRepository {

    private static final Logger LOGGER = LogManager.getLogger(HdfsRepository.class);

    // buffer size passed to HDFS read/write methods
    // TODO: why 100KB?
    private static final ByteSizeValue DEFAULT_BUFFER_SIZE = new ByteSizeValue(100, ByteSizeUnit.KB);

    private static final Setting<ByteSizeValue> BUFFER_SIZE_SETTING =
        Setting.byteSizeSetting("buffer_size", DEFAULT_BUFFER_SIZE);

    // We cannot use a ByteSize setting as it doesn't support NULL
    // and it must be NULL as default to indicate to not override the default behaviour.
    private static final Setting<String> CHUNK_SIZE_SETTING = Setting.simpleString("chunk_size",  Setting.Property.NodeScope);

    private static final Setting<String> URI_SETTING = Setting.simpleString("uri", Setting.Property.NodeScope);

    private static final Setting<String> PATH_SETTING = Setting.simpleString("path", Setting.Property.NodeScope);

    private static final Setting<String> SECURITY_PRINCIPAL_SETTING = Setting.simpleString("security.principal", Setting.Property.NodeScope);

    private static final Setting<Boolean> LOAD_DEFAULTS_SETTING = Setting.boolSetting("load_defaults",true, Setting.Property.NodeScope);

    public static List<Setting> settingsToValidate() {
        return List.of(URI_SETTING,
                       SECURITY_PRINCIPAL_SETTING,
                       PATH_SETTING,
                       LOAD_DEFAULTS_SETTING,
                       COMPRESS_SETTING,
                       CHUNK_SIZE_SETTING);
    }

    private final Environment environment;
    private final ByteSizeValue chunkSize;
    private final BlobPath basePath = BlobPath.cleanPath();
    private final URI uri;
    private final String pathSetting;


    public HdfsRepository(RepositoryMetaData metadata, Environment environment,
                          NamedXContentRegistry namedXContentRegistry) {
        super(metadata, environment.settings(), namedXContentRegistry);

        this.environment = environment;
        this.chunkSize = metadata.settings().getAsBytesSize(CHUNK_SIZE_SETTING.getKey(), null);

        String uriSetting = URI_SETTING.get(metadata.settings());
        if (Strings.hasText(uriSetting) == false) {
            throw new IllegalArgumentException("No 'uri' defined for hdfs snapshot/restore");
        }
        uri = URI.create(uriSetting);
        if ("hdfs".equalsIgnoreCase(uri.getScheme()) == false) {
            throw new IllegalArgumentException(String.format(Locale.ROOT,
                "Invalid scheme [%s] specified in uri [%s]; only 'hdfs' uri allowed for hdfs snapshot/restore",
                uri.getScheme(), uriSetting));
        }
        if (Strings.hasLength(uri.getPath()) && uri.getPath().equals("/") == false) {
            throw new IllegalArgumentException(String.format(Locale.ROOT,
                "Use 'path' option to specify a path [%s], not the uri [%s] for hdfs snapshot/restore", uri.getPath(), uriSetting));
        }

        pathSetting = PATH_SETTING.get(getMetadata().settings());
        // get configuration
        if (pathSetting == null) {
            throw new IllegalArgumentException("No 'path' defined for hdfs snapshot/restore");
        }
    }

    private HdfsBlobStore createBlobstore(URI uri, String path, Settings repositorySettings)  {
        Configuration hadoopConfiguration = new Configuration(LOAD_DEFAULTS_SETTING.get(repositorySettings));
        hadoopConfiguration.setClassLoader(HdfsRepository.class.getClassLoader());
        hadoopConfiguration.reloadConfiguration();

        final Settings confSettings = repositorySettings.getByPrefix("conf.");
        for (String key : confSettings.keySet()) {
            LOGGER.debug("Adding configuration to HDFS Client Configuration : {} = {}", key, confSettings.get(key));
            hadoopConfiguration.set(key, confSettings.get(key));
        }

        // Disable FS cache
        hadoopConfiguration.setBoolean("fs.hdfs.impl.disable.cache", true);

        // Create a hadoop user
        UserGroupInformation ugi = login(hadoopConfiguration, repositorySettings);

        // Sense if HA is enabled
        // HA requires elevated permissions during regular usage in the event that a failover operation
        // occurs and a new connection is required.
        String host = uri.getHost();
        String configKey = HdfsClientConfigKeys.Failover.PROXY_PROVIDER_KEY_PREFIX + "." + host;
        Class<?> ret = hadoopConfiguration.getClass(configKey, null, FailoverProxyProvider.class);
        boolean haEnabled = ret != null;

        int bufferSize = BUFFER_SIZE_SETTING.get(repositorySettings).bytesAsInt();

        // Create the filecontext with our user information
        // This will correctly configure the filecontext to have our UGI as its internal user.
        FileContext fileContext = ugi.doAs((PrivilegedAction<FileContext>) () -> {
            try {
                AbstractFileSystem fs = AbstractFileSystem.get(uri, hadoopConfiguration);
                return FileContext.getFileContext(fs, hadoopConfiguration);
            } catch (UnsupportedFileSystemException e) {
                throw new UncheckedIOException(e);
            }
        });

        LOGGER.debug("Using file-system [{}] for URI [{}], path [{}]", fileContext.getDefaultFileSystem(),
                fileContext.getDefaultFileSystem().getUri(), path);

        try {
            return new HdfsBlobStore(fileContext, path, bufferSize, isReadOnly(), haEnabled);
        } catch (IOException e) {
            throw new UncheckedIOException(String.format(Locale.ROOT, "Cannot create HDFS repository for uri [%s]", uri), e);
        }
    }

    private UserGroupInformation login(Configuration hadoopConfiguration, Settings repositorySettings) {
        // Validate the authentication method:
        AuthenticationMethod authMethod = SecurityUtil.getAuthenticationMethod(hadoopConfiguration);
        if (authMethod.equals(AuthenticationMethod.SIMPLE) == false
            && authMethod.equals(AuthenticationMethod.KERBEROS) == false) {
            throw new RuntimeException("Unsupported authorization mode ["+authMethod+"]");
        }

        // Check if the user added a principal to use, and that there is a keytab file provided
        String kerberosPrincipal = SECURITY_PRINCIPAL_SETTING.get(repositorySettings);

        // Check to see if the authentication method is compatible
        if (kerberosPrincipal != null && authMethod.equals(AuthenticationMethod.SIMPLE)) {
            LOGGER.warn("Hadoop authentication method is set to [SIMPLE], but a Kerberos principal is " +
                "specified. Continuing with [KERBEROS] authentication.");
            SecurityUtil.setAuthenticationMethod(AuthenticationMethod.KERBEROS, hadoopConfiguration);
        } else if (kerberosPrincipal == null && authMethod.equals(AuthenticationMethod.KERBEROS)) {
            throw new RuntimeException("HDFS Repository does not support [KERBEROS] authentication without " +
                "a valid Kerberos principal and keytab. Please specify a principal in the repository settings with [" +
                SECURITY_PRINCIPAL_SETTING.getKey() + "].");
        }

        // Now we can initialize the UGI with the configuration.
        UserGroupInformation.setConfiguration(hadoopConfiguration);

        // Debugging
        LOGGER.debug("Hadoop security enabled: [{}]", UserGroupInformation.isSecurityEnabled());
        LOGGER.debug("Using Hadoop authentication method: [{}]", SecurityUtil.getAuthenticationMethod(hadoopConfiguration));

        // UserGroupInformation (UGI) instance is just a Hadoop specific wrapper around a Java Subject
        try {
            if (UserGroupInformation.isSecurityEnabled()) {
                String principal = preparePrincipal(kerberosPrincipal);
                String keytab = HdfsSecurityContext.locateKeytabFile(environment).toString();
                LOGGER.debug("Using kerberos principal [{}] and keytab located at [{}]", principal, keytab);
                return UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keytab);
            }
            return UserGroupInformation.getCurrentUser();
        } catch (IOException e) {
            throw new UncheckedIOException("Could not retrieve the current user information", e);
        }
    }

    // Convert principals of the format 'service/_HOST@REALM' by subbing in the local address for '_HOST'.
    private static String preparePrincipal(String originalPrincipal) {
        String finalPrincipal = originalPrincipal;
        // Don't worry about host name resolution if they don't have the _HOST pattern in the name.
        if (originalPrincipal.contains("_HOST")) {
            try {
                finalPrincipal = SecurityUtil.getServerPrincipal(originalPrincipal, getHostName());
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }

            if (originalPrincipal.equals(finalPrincipal) == false) {
                LOGGER.debug("Found service principal. Converted original principal name [{}] to server principal [{}]",
                    originalPrincipal, finalPrincipal);
            }
        }
        return finalPrincipal;
    }

    @SuppressForbidden(reason = "InetAddress.getLocalHost(); Needed for filling in hostname for a kerberos principal name pattern.")
    private static String getHostName() {
        try {
            /*
             * This should not block since it should already be resolved via Log4J and Netty. The
             * host information is cached by the JVM and the TTL for the cache entry is infinite
             * when the SecurityManager is activated.
             */
            return InetAddress.getLocalHost().getCanonicalHostName();
        } catch (UnknownHostException e) {
            throw new RuntimeException("Could not locate host information", e);
        }
    }

    @Override
    protected HdfsBlobStore createBlobStore() {
        // initialize our blobstore using elevated privileges.
        final HdfsBlobStore blobStore =
            AccessController.doPrivileged((PrivilegedAction<HdfsBlobStore>)
                () -> createBlobstore(uri, pathSetting, getMetadata().settings()));
        return blobStore;
    }

    @Override
    protected BlobPath basePath() {
        return basePath;
    }

    @Override
    protected ByteSizeValue chunkSize() {
        return chunkSize;
    }
}
