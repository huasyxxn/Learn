package com.huangys.flink.util;

//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

import java.util.HashMap;
import java.util.Map;
import java.util.Stack;

public class Base62 {
    private static Map<String, Integer> charSetMap = new HashMap();
    private static char[] charSet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz".toCharArray();

    public Base62() {
    }

    public static String encode(long number) {
        return encode(number, 0);
    }

    public static String encode(long number, int length) {
        Long rest = Long.valueOf(number);
        Stack stack = new Stack();

        StringBuilder result;
        for(result = new StringBuilder(0); rest.longValue() != 0L; rest = Long.valueOf(rest.longValue() / 62L)) {
            stack.add(Character.valueOf(charSet[(new Long(rest.longValue() - rest.longValue() / 62L * 62L)).intValue()]));
        }

        while(!stack.isEmpty()) {
            result.append(stack.pop());
        }

        int result_length = result.length();
        StringBuilder temp0 = new StringBuilder();

        for(int base62 = 0; base62 < length - result_length; ++base62) {
            temp0.append('0');
        }

        String var9 = temp0.toString() + result.toString();
        return var9;
    }

    public static long decode(String base62) throws Exception {
        Long dst = Long.valueOf(0L);

        for(int i = 0; i < base62.length(); ++i) {
            char c = base62.charAt(i);
            if(!charSetMap.containsKey(String.valueOf(c))) {
                throw new Exception("输入的字符串并非base62字符串，因为包含非法字符" + String.valueOf(c));
            }

            dst = Long.valueOf(dst.longValue() * 62L + (long)((Integer)charSetMap.get(String.valueOf(c))).intValue());
        }

        return dst.longValue();
    }

    static {
        for(int i = 0; i < charSet.length; ++i) {
            charSetMap.put(String.valueOf(charSet[i]), Integer.valueOf(i));
        }

    }
}
