package luthfan_simple_db;

public class CommandResult {
    public static final int OK = 1;
    public static final int ERROR = 2;
    public static final int EXIT = 3;

    public int status = CommandResult.OK;
    public String message = "";
}
