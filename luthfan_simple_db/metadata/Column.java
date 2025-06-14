package luthfan_simple_db.metadata;

import java.io.Serializable;

public class Column implements Serializable {
    public static int VARCHAR = 1;

    public String name;
    public int dataType;
    public int length;
    public String dataTypeName;
}
