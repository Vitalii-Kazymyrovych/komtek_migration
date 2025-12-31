package com.incoresoft.releasesmigrator;

import java.io.*;

/**
 * Copied from plugin-commons-lib: {@code com.incoresoft.plugin.commons.serialization.JavaObjectSerializer}.
 */
public class JavaObjectSerializer {
    public static byte[] getData(Object obj) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream out = new ObjectOutputStream(bos);
        out.writeObject(obj);
        out.flush();

        return bos.toByteArray();
    }

    public static Object getObject(byte[] data) throws IOException, ClassNotFoundException {
        ByteArrayInputStream bis = new ByteArrayInputStream(data);
        ObjectInputStream in = new ObjectInputStream(bis);
        return in.readObject();
    }
}
