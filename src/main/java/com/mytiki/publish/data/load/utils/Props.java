/*
 * Copyright (c) TIKI Inc.
 * MIT license. See LICENSE file in root directory.
 */

package com.mytiki.publish.data.load.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class Props {
    public static Properties load(String filename) {
        try (InputStream input = Props.class
                .getClassLoader()
                .getResourceAsStream(filename)) {
            Properties properties = new Properties();
            properties.load(input);
            return properties;
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public static Properties withEnv(String filename) {
        Properties properties = Props.load(filename);
        Map<String, String> env = new HashMap<>();
        properties.keySet().forEach((k) -> {
            String key = String.valueOf(k);
            String value = Env.get(Env.name(key));
            if (value != null) env.put(key, value);
        });
        env.forEach(properties::setProperty);
        return properties;
    }
}
