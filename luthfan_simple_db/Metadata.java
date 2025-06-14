package luthfan_simple_db;

import java.io.Serializable;
import java.util.*;
import luthfan_simple_db.metadata.*;

public class Metadata implements Serializable {
    public Map<String, Table> tables = new HashMap<>();
}
