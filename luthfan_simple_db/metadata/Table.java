package luthfan_simple_db.metadata;

import java.io.Serializable;
import java.util.*;

public class Table implements Serializable {
    public Map<String, Column> columns = new LinkedHashMap<>();
    public String name;
}
