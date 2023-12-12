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
