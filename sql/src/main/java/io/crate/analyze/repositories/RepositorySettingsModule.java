/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.analyze.repositories;

import com.google.common.collect.ImmutableMap;
import io.crate.common.collections.Maps;
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.GenericProperties;
import io.crate.sql.tree.GenericProperty;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.multibindings.MapBinder;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.repositories.s3.S3ClientSettings;
import org.elasticsearch.repositories.s3.S3Repository;
import org.elasticsearch.repositories.url.URLRepository;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;


public class RepositorySettingsModule extends AbstractModule {

    private static final String FS = "fs";
    private static final String URL = "url";
    private static final String HDFS = "hdfs";
    private static final String S3 = "s3";

    private static final TypeSettings FS_SETTINGS = new TypeSettings(
        convertSettingListToMap(FsRepository.settingsToValidate()),
        convertSettingListToMap(FsRepository.optionalSettingsToValidate())
    );

    private static final TypeSettings URL_SETTINGS = new TypeSettings(
        convertSettingListToMap(URLRepository.settingsToValidate()),
        Map.of()
    );

    private static final TypeSettings HDFS_SETTINGS = new TypeSettings(
        Collections.emptyMap(),
        ImmutableMap.<String, Setting>builder()
            .put("uri", Setting.simpleString("uri", Setting.Property.NodeScope))
            .put("security.principal", Setting.simpleString("security.principal", Setting.Property.NodeScope))
            .put("path", Setting.simpleString("path", Setting.Property.NodeScope))
            .put("load_defaults", Setting.boolSetting("load_defaults", true, Setting.Property.NodeScope))
            .put("concurrent_streams", Setting.intSetting("concurrent_streams", 5, Setting.Property.NodeScope))
            .put("compress", Setting.boolSetting("compress", true, Setting.Property.NodeScope))
            // We cannot use a ByteSize setting as it doesn't support NULL and it must be NULL as default to indicate to
            // not override the default behaviour.
            .put("chunk_size", Setting.simpleString("chunk_size"))
            .build()) {

        @Override
        public GenericProperties dynamicProperties(GenericProperties genericProperties) {
            if (genericProperties.isEmpty()) {
                return genericProperties;
            }
            GenericProperties dynamicProperties = new GenericProperties();
            for (Map.Entry<String, Expression> entry : genericProperties.properties().entrySet()) {
                String key = entry.getKey();
                if (key.startsWith("conf.")) {
                    dynamicProperties.add(new GenericProperty(key, entry.getValue()));
                }
            }
            return dynamicProperties;
        }
    };

    private static final TypeSettings S3_SETTINGS = new TypeSettings(
        Map.of(),
        Maps.concat(
            convertSettingListToMap(S3Repository.optionalSettingsToValidate()),
            // client related settings
            convertSettingListToMap(renameSettingsUsingSuffixAsKey(S3ClientSettings.optionalSettingsToValidate()))
        ));

    @Override
    protected void configure() {
        MapBinder<String, TypeSettings> typeSettingsBinder = MapBinder.newMapBinder(
            binder(),
            String.class,
            TypeSettings.class);
        typeSettingsBinder.addBinding(FS).toInstance(FS_SETTINGS);
        typeSettingsBinder.addBinding(URL).toInstance(URL_SETTINGS);
        typeSettingsBinder.addBinding(HDFS).toInstance(HDFS_SETTINGS);
        typeSettingsBinder.addBinding(S3).toInstance(S3_SETTINGS);
    }

    private static Map<String, Setting> convertSettingListToMap(List<Setting> settingList) {
        return settingList.stream().collect(Collectors.toMap(Setting::getKey, Function.identity()));
    }

    /**
     * Copy and rename {@link Setting}s.
     * For each Setting, a new Setting is created
     * with it's key suffix acting as the new key
     *
     * eg. From Setting: s3.client.default.endpoint
     * a new Setting will be created with same characteristics
     * and new key name: endpoint
     */
    private static List<Setting> renameSettingsUsingSuffixAsKey(List<Setting> settingList) {
        return settingList
            .stream()
            .map(s -> s.copyAndRename(k -> ((String) k).substring(((String) k).lastIndexOf('.') + 1)))
            .collect(Collectors.toList());
    }
}
