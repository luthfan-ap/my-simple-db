package luthfan_simple_db.engine;

import luthfan_simple_db.metadata.*;
import java.util.*;
import java.io.*;

public class RowSerializer {
    public static int getRowSize(Table table) {
        int size = 0;
        for (Column c : table.columns.values()) {
            size += c.length;
        }
        return size;
    }

    public static byte[] serialize(Map<String, String> row, Table table) throws UnsupportedEncodingException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (Column col : table.columns.values()) {
            String val = row.get(col.name);
            byte[] data = val.getBytes("UTF-8");
            byte[] fixed = Arrays.copyOf(data, col.length);
            out.write(fixed, 0, col.length);
        }
        return out.toByteArray();
    }

    public static Map<String, String> deserialize(byte[] bytes, Table table) throws UnsupportedEncodingException {
        Map<String, String> row = new LinkedHashMap<>();
        int offset = 0;
        for (Column col : table.columns.values()) {
            byte[] fieldBytes = Arrays.copyOfRange(bytes, offset, offset + col.length);
            row.put(col.name, new String(fieldBytes, "UTF-8").trim());
            offset += col.length;
        }
        return row;
    }
}
