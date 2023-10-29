/*
 * Copyright (c) TIKI Inc.
 * MIT license. See LICENSE file in root directory.
 */

package com.mytiki.publish.data.load.utils;

public class Env {
    public static String get(String var) {
        return System.getenv(var);
    }

    public static String name(String var) {
        return var
                .replace(".", "_")
                .replace("-", "_")
                .toUpperCase();
    }
}
