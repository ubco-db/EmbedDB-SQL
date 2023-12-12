/**
 * @file		Example.java
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

import java.sql.SQLException;

public class Example {
    public static void main(String[] args) throws SQLException, InterruptedException {
        for (int query = 1; query <= 4; query++) {
            System.out.printf("""
                ┌───────────────────────────────────────────┐
                │       EmbedDB SQL Converter Example       │
                │                 Query %d                   │
                └───────────────────────────────────────────┘
                
                    """, query);

            try (SQLConverter sqlConverter = getSqlConverter(query)) {

                String select = getSelectQuery(query);

                System.out.println(sqlConverter.toCCode(select));
            } catch (Exception e) {
                System.err.println(e.getMessage());
            }
        }
    }

    private static String getSelectQuery(int query) {
        return switch (query) {
            // Min, max, avg daily temperatures
            case 1 -> """
                    SELECT floor(key / 86400) as "Day",
                    min(temp), max(temp), avg(temp)
                    FROM uwa
                    GROUP BY "Day"
                    """;
            // Avg. temp on days with wind speed > 15 mph
            case 2 -> """
                    SELECT floor(key / 86400) as "Day",
                    avg(temp), max(wind)
                    FROM uwa
                    GROUP BY "Day"
                    HAVING max(wind) > 150
                    """;
            // Records with ethylene concentration > 0
            case 3 -> """
                    SELECT COUNT(*)
                    FROM ethylene
                    WHERE conc > 0
                    """;
            // Count records with X motion above 5e8
            case 4 -> """
                    SELECT floor(key / 706000) as "Bucket", COUNT(*) as "Count"
                    FROM watch
                    WHERE x > 500000000
                    GROUP BY "Bucket"
                    """;
            default -> throw new IllegalArgumentException();
        };
    }

    private static SQLConverter getSqlConverter(int query) throws SQLException {
        String[] uwaDDL = {"""
                CREATE TABLE uwa (
                    key INT PRIMARY KEY,
                    temp INT,
                    pres INT,
                    wind INT
                )
                """,
                "CREATE INDEX uTemp ON uwa (temp)"
        };

        String[] ethDDL = {"""
                CREATE TABLE ethylene (
                    key INT PRIMARY KEY,
                    col1 INT,
                    conc INT,
                    col3 INT
                )
                """,
                "CREATE INDEX eConc ON ethylene (conc)"
        };

        String[] watchDDL = {"""
                CREATE TABLE watch (
                    key INT PRIMARY KEY,
                    x INT,
                    y INT,
                    z INT
                );
                """,
                "CREATE INDEX watchX ON watch (x)"
        };

        return new SQLConverter(
                switch (query) {
                    case 1, 2 -> uwaDDL;
                    case 3 -> ethDDL;
                    case 4 -> watchDDL;
                    default -> throw new IllegalArgumentException();
                }
        );
    }
}
