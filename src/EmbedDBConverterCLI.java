/**
 * @file		EmbedDBConverterCLI.java
 * @author		EmbedDB Team
 * @brief		Source code for EmbedDB-SQL
 * @copyright	Copyright 2023
 * 			    EmbedDB Team
 * @par Redistribution and use in source and binary forms, with or without
 * 	modification, are permitted provided that the following conditions are met:
 *
 * @par 1.Redistributions of source code must retain the above copyright notice,
 * 	this list of conditions and the following disclaimer.
 *
 * @par 2.Redistributions in binary form must reproduce the above copyright notice,
 * 	this list of conditions and the following disclaimer in the documentation
 * 	and/or other materials provided with the distribution.
 *
 * @par 3.Neither the name of the copyright holder nor the names of its contributors
 * 	may be used to endorse or promote products derived from this software without
 * 	specific prior written permission.
 *
 * @par THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * 	AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * 	IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * 	ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * 	LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * 	CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * 	SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * 	INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * 	CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * 	ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * 	POSSIBILITY OF SUCH DAMAGE.
 */

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
