import static dBEngine.command.CommandParser.isExit;
import static dBEngine.command.CommandParser.parseUserCommand;
import static dBEngine.sdl.DataStoreUtil.initializeDataStore;
import static dBEngine.util.Constants.COPYRIGHT;
import static dBEngine.util.Constants.PROMPT;
import static dBEngine.util.Constants.VERSION;
import static dBEngine.util.PrintUtil.line;

import java.util.Scanner;

public class Prompt {
	static Scanner scanner = new Scanner(System.in).useDelimiter(";");

	public static void main(String[] args) {

		try {
			initializeDataStore();
			splashScreen();

			String userCommand = "";

			while (!isExit) {
				System.out.print(PROMPT);
				userCommand = scanner.next().replace("\n", " ").replace("\r", " ").trim().toLowerCase();
				parseUserCommand(userCommand);
			}
			System.out.println("Exiting...");
		} catch (Exception e) {
			System.out.println(e);
			e.printStackTrace();
		}

	}

	public static void splashScreen() {
		System.out.println(line("-", 80));
		System.out.println("Welcome to DavisBaseLite"); // Display the string.
		System.out.println("DavisBaseLite Version " + VERSION);
		System.out.println(COPYRIGHT);
		System.out.println("\nType \"help;\" to display supported commands.");
		System.out.println(line("-", 80));
	}
}
