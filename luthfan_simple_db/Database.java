package luthfan_simple_db;
import luthfan_simple_db.metadata.*;
import luthfan_simple_db.engine.RowSerializer;
import java.io.*;
import java.util.*;

public class Database {
    public Scanner sc;
    public String input;
    public Metadata metadata;

    // Database (masuk ke input database)
    public Database() {
        sc = new Scanner(System.in);
        loadDatabase();
    }

    public void loadDatabase() {
        try (ObjectInputStream in = new ObjectInputStream(new FileInputStream("simpledb.data"))) {
            metadata = (Metadata) in.readObject();
            System.out.println("Loaded existing metadata.");
        } catch (IOException | ClassNotFoundException e) {
            metadata = new Metadata();
            System.out.println("No saved metadata found, starting fresh.");
        }
    }

    public void saveDatabase() {
        try (ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream("simpledb.data"))) {
            out.writeObject(metadata);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void start() {
        while (true) {
            System.out.print("simpledb > ");
            input = sc.nextLine().trim().toLowerCase();
            try {
                if (input.startsWith(".")) {
                    CommandResult cr = doMetaCommand();
                    if (cr.status == CommandResult.EXIT) break;
                } else {
                    Statement stmt = prepareStatement();
                    executeStatement(stmt);
                }
            } catch (Exception e) {
                System.out.println("Error: " + e.getMessage());
            }
        }
        saveDatabase();
        sc.close();
    }

    public CommandResult doMetaCommand() {
        CommandResult cr = new CommandResult();
        if (input.equals(".exit")) {
            cr.status = CommandResult.EXIT;
        } else if (input.equals(".tables")) {
            cr.status = CommandResult.OK;
            metadata.tables.keySet().forEach(System.out::println);
        } else if (input.startsWith(".timeline")) {
            String[] parts = input.split(" ");
            if (parts.length < 2) {
                System.out.println("Usage: .timeline <username>");
            } else {
                try {
                    handleTimeline(parts[1]);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            cr.status = CommandResult.OK;
        }
        return cr;
    }

    public Statement prepareStatement() throws Exception {
        Statement stmt = new Statement();
        stmt.query = input;

        if (input.startsWith("create")) stmt.type = Statement.CREATE;
        else if (input.startsWith("insert")) stmt.type = Statement.INSERT;
        else if (input.startsWith("select")) stmt.type = Statement.SELECT;
        else throw new Exception("Unsupported command.");

        stmt.parserQuery = input.split(" ");
        return stmt;
    }

    public void executeStatement(Statement stmt) throws Exception {
        if (stmt.type == Statement.CREATE) {
            handleCreate(stmt);
        } else if (stmt.type == Statement.INSERT) {
            handleInsert(stmt);
        } else if (stmt.type == Statement.SELECT) {
            handleSelect(stmt);
        }
    }

    private void handleCreate(Statement stmt) throws Exception {
        String tableName = stmt.parserQuery[2];
        Table table = new Table();
        table.name = tableName;

        for (int i = 3; i < stmt.parserQuery.length; i += 3) {
            Column c = new Column();
            c.name = stmt.parserQuery[i];
            c.dataTypeName = stmt.parserQuery[i + 1];
            c.dataType = Column.VARCHAR;
            c.length = Integer.parseInt(stmt.parserQuery[i + 2]);
            table.columns.put(c.name, c);
        }

        metadata.tables.put(table.name, table);
        new File("luthfan_simple_db/db").mkdir(); // ensure db folder exists
        new RandomAccessFile("luthfan_simple_db/db/" + tableName + ".db", "rw").close(); // create empty file
        System.out.println("Table " + tableName + " created.");
    }

    private void handleInsert(Statement stmt) throws Exception {
        String[] parts = stmt.query.split(" ");
        String tableName = parts[2];
        Table table = metadata.tables.get(tableName);
        if (table == null) throw new Exception("Table not found.");

        Map<String, String> row = new LinkedHashMap<>();
        int i = 3;
        for (String colName : table.columns.keySet()) {
            row.put(colName, parts[i++]);
        }

        byte[] serialized = RowSerializer.serialize(row, table);
        try (RandomAccessFile file = new RandomAccessFile("luthfan_simple_db/db/" + tableName + ".db", "rw")) {
            file.seek(file.length());
            long offset = file.length(); // sebelum write
            file.write(serialized);

            if (tableName.equals("posts")) {
                String user = row.get("user");
                userIndex.computeIfAbsent(user, k -> new ArrayList<>()).add(offset);
                saveIndex("posts", "user");
            }
        }

        System.out.println("Inserted 1 row into " + tableName);
    }

    private void handleSelect(Statement stmt) throws Exception {
        String[] parts = stmt.query.split(" ");
        String tableName = parts[3];
        Table table = metadata.tables.get(tableName);
        if (table == null) throw new Exception("Table not found.");

        String whereColumn = null;
        String whereValue = null;

        // Check for WHERE clause
        if (stmt.query.contains("where")) {
            int whereIndex = Arrays.asList(parts).indexOf("where");
            whereColumn = parts[whereIndex + 1];
            whereValue = parts[whereIndex + 3]; // assumes "=" is always present
        }

        int rowSize = RowSerializer.getRowSize(table);

        // Check for indexed select on posts.user
        if ("posts".equals(tableName) && "user".equals(whereColumn)) {
            loadIndex("posts", "user"); // load index file into memory
            List<Long> offsets = userIndex.get(whereValue);

            if (offsets == null || offsets.isEmpty()) {
                System.out.println("No rows found.");
                return;
            }

            try (RandomAccessFile file = new RandomAccessFile("luthfan_simple_db/db/" + tableName + ".db", "r")) {
                byte[] buffer = new byte[rowSize];
                for (long offset : offsets) {
                    file.seek(offset);
                    file.readFully(buffer);
                    Map<String, String> row = RowSerializer.deserialize(buffer, table);
                    System.out.println(row);
                }
            }
            return;
        }

        // Default full scan if not using index
        try (RandomAccessFile file = new RandomAccessFile("luthfan_simple_db/db/" + tableName + ".db", "r")) {
            byte[] buffer = new byte[rowSize];
            while (file.getFilePointer() < file.length()) {
                long currentOffset = file.getFilePointer();
                file.readFully(buffer);
                Map<String, String> row = RowSerializer.deserialize(buffer, table);

                if (whereColumn == null || row.get(whereColumn).equals(whereValue)) {
                    System.out.println(row);
                }
            }
        }
    }

    public void handleTimeline(String user) throws Exception {
        // 1. Load followees
        Table followsTable = metadata.tables.get("follows");
        if (followsTable == null) {
            System.out.println("Follows table not found.");
            return;
        }

        Set<String> followees = new HashSet<>();
        int rowSize = RowSerializer.getRowSize(followsTable);

        try (RandomAccessFile file = new RandomAccessFile("luthfan_simple_db/db/follows.db", "r")) {
            byte[] buffer = new byte[rowSize];
            while (file.getFilePointer() < file.length()) {
                file.readFully(buffer);
                Map<String, String> row = RowSerializer.deserialize(buffer, followsTable);
                if (row.get("follower").equals(user)) {
                    followees.add(row.get("followee"));
                }
            }
        }

        if (followees.isEmpty()) {
            System.out.println(user + " is not following anyone.");
            return;
        }

        // 2. Load posts
        Table postsTable = metadata.tables.get("posts");
        if (postsTable == null) {
            System.out.println("Posts table not found.");
            return;
        }

        int postSize = RowSerializer.getRowSize(postsTable);

        try (RandomAccessFile file = new RandomAccessFile("luthfan_simple_db/db/posts.db", "r")) {
            byte[] buffer = new byte[postSize];
            boolean found = false;
            while (file.getFilePointer() < file.length()) {
                file.readFully(buffer);
                Map<String, String> row = RowSerializer.deserialize(buffer, postsTable);
                if (followees.contains(row.get("user"))) {
                    System.out.println(row);
                    found = true;
                }
            }

            if (!found) {
                System.out.println("No posts from followed users.");
            }
        }
    }

    public Map<String, List<Long>> userIndex = new HashMap<>();

    @SuppressWarnings("unchecked")
    public void loadIndex(String tableName, String column) {
        String path = "luthfan_simple_db/db/" + tableName + "_" + column + ".index";
        File file = new File(path);
        if (!file.exists()) return;

        try (ObjectInputStream in = new ObjectInputStream(new FileInputStream(file))) {
            userIndex = (Map<String, List<Long>>) in.readObject();
            System.out.println("Index loaded: " + column);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void saveIndex(String tableName, String column) {
        try (ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream("luthfan_simple_db/db/" + tableName + "_" + column + ".index"))) {
            out.writeObject(userIndex);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public static void main(String[] args) {
        new Database().start();
    }

}
