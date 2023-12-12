import SQLConverter.SQLConverter;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Scanner;

public class EmbedDBConverterCLI {
    public static void main(String[] args) throws SQLException, InterruptedException {
        try (SQLConverter sqlConverter = new SQLConverter()) {
            Scanner scanner = new Scanner(System.in);

            // Print use instructions
            System.out.println("""
                    ┌──────────────────────────────────────────┐
                    │        EmbedDB SQL Converter CLI         │
                    │ Enter SQL statements, ending each with ; │
                    │           Type "exit" to leave           │
                    └──────────────────────────────────────────┘
                    """);

            while (true) {
                System.out.print(">>> ");

                StringBuilder sb = new StringBuilder(scanner.nextLine());
                while (sb.charAt(sb.length() - 1) != ';') {
                    System.out.print("... ");
                    sb.append("\n").append(scanner.nextLine());
                }

                String input = sb.toString();

                if (input.toLowerCase().startsWith("exit")) {
                    break;
                }

                if (input.toLowerCase().startsWith("select")) {
                    try {
                        System.out.println(sqlConverter.toCCode(input));
                    } catch (Exception e) {
                        System.err.println(e.getMessage());
                    }
                    Thread.sleep(100);
                    continue;
                }

                try {
                    sqlConverter.executeDDL(input);
                } catch (SQLException e) {
                    System.err.println(e.getMessage());
                    // Wait a bit otherwise sometimes prints are out of order
                    Thread.sleep(100);
                }
            }
        } catch (IOException e) {
            System.err.println(e.getMessage());
        }
    }
}
