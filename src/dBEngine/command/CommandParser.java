package dBEngine.command;

import static dBEngine.util.Constants.COPYRIGHT;
import static dBEngine.util.Constants.VERSION;
import static dBEngine.util.PrintUtil.line;
import static java.lang.System.out;

import java.util.ArrayList;
import java.util.Arrays;

import dBEngine.ddl.CreateTable;
import dBEngine.ddl.DropTable;
import dBEngine.ddl.ShowTables;
import dBEngine.dml.DeleteFrom;
import dBEngine.dml.InsertInto;
import dBEngine.dml.Update;
import dBEngine.qdl.SelectFrom;

public class CommandParser {
	public static boolean isExit = false;

	public static void parseUserCommand(String userCommand) {
		userCommand = userCommand.replaceAll("\n", " "); // Remove newlines
		userCommand = userCommand.replaceAll("\r", " "); // Remove carriage returns
		userCommand = userCommand.replaceAll(",", " , "); // Tokenize commas
		userCommand = userCommand.replaceAll("\\(", " ( "); // Tokenize left parentheses
		userCommand = userCommand.replaceAll("\\)", " ) "); // Tokenize right parentheses
		userCommand = userCommand.replaceAll("\\=", " = ");
		userCommand = userCommand.replaceAll(">=", " >= ");
		userCommand = userCommand.replaceAll("<=", " <= ");
		userCommand = userCommand.replaceAll("<", " < ");
		userCommand = userCommand.replaceAll(">", " > ");
		userCommand = userCommand.replaceAll("\"", "");
		userCommand = userCommand.replaceAll("\'", "");
		userCommand = userCommand.replaceAll("( )+", " "); // Reduce multiple spaces to a single space

		ArrayList<String> commandTokens = new ArrayList<String>(Arrays.asList(userCommand.split(" ")));

		try {
			switch (commandTokens.get(0)) {
			case "show":
				ShowTables.run(commandTokens);
				break;
			case "select":
				SelectFrom.run(commandTokens);
				break;
			case "drop":
				DropTable.run(commandTokens);
				break;
			case "create":
				CreateTable.run(commandTokens);
				break;
			case "update":
				Update.run(commandTokens);
				break;
			case "insert":
				InsertInto.run(commandTokens);
				break;
			case "delete":
				DeleteFrom.run(commandTokens);
				break;
			case "help":
				help();
				break;
			case "version":
				displayVersion();
				break;
			case "exit":
				isExit = true;
				break;
			case "quit":
				isExit = true;
				break;
			default:
				System.out.println("I didn't understand the command: \"" + userCommand + "\"");
				break;
			}
		} catch (Exception e) {
			System.out.println(e);
			e.printStackTrace();
		}
	}

	public static void help() {
		out.println(line("*", 80));
		out.println("SUPPORTED COMMANDS\n");
		out.println("All commands below are case insensitive\n");
		out.println("SHOW TABLES;");
		out.println("\tDisplay the names of all tables.\n");
		out.println("CREATE TABLE <table_name>;");
		out.println("\tCreates table as per the schema specified.\n");
		out.println("SELECT <column_list> FROM <table_name> [WHERE <condition>];");
		out.println("\tDisplay table records whose optional <condition>");
		out.println("\tis <column_name> = <value>.\n");
		out.println("DROP TABLE <table_name>;");
		out.println("\tRemove table data (i.e. all records) and its schema.\n");
		out.println("UPDATE TABLE <table_name> SET <column_name> = <value> [WHERE <condition>];");
		out.println("\tModify records data whose optional <condition>");
		out.println("\tis <column_name> = <value>.\n");
		out.println("INSERT INTO <table_name> <column_list> values(<value_list>);");
		out.println("\tInsert data into table.\n");
		out.println("DELETE FROM <table_name> [WHERE <condition>];");
		out.println("\tDeletes record data whose optional <condition> is");
		out.println("\tis <column_name> = <value>.\n");
		out.println("VERSION;");
		out.println("\tDisplay the program version.\n");
		out.println("HELP;");
		out.println("\tDisplay this help information.\n");
		out.println("EXIT;");
		out.println("\tExit the program.\n");
		out.println(line("*", 80));
	}

	public static void displayVersion() {
		System.out.println("DavisBaseLite Version " + VERSION);
		System.out.println(COPYRIGHT);
	}
}
