package com.izuanqian.ilivespider;

import java.text.MessageFormat;

/**
 * @author sanlion do
 */
public class Key {

    /**
     * 拼接key
     *
     * @param pattern
     * @param params
     * @return
     */
    public static String __(String pattern, Object... params) {
        return MessageFormat.format(pattern, params);
    }
}
