import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import SQLConverter.SQLConverter;

@SuppressWarnings("CallToPrintStackTrace")
public class TestSQLConverter {
    private static SQLConverter sqlConverter;

    @BeforeAll
    public static void setUp() throws Exception {
        // Setup stmt by running ddl statements
        sqlConverter = new SQLConverter("CREATE TABLE uwa (id INT PRIMARY KEY, airTemp INT, airPres INT, windSpeed INT)", "CREATE TABLE sea (id INT PRIMARY KEY, airTemp INT, airPres INT, windSpeed INT)", "CREATE INDEX uTemp ON uwa (airTemp)", "CREATE INDEX sTemp ON sea (airTemp)");
    }

    @ParameterizedTest
    @MethodSource("provideQueryTestData")
    public void testQuery(String query, String resultFile) throws IOException {
        String result = null;
        try {
            result = sqlConverter.toCCode(query);
        } catch (Exception e) {
            e.printStackTrace();
            fail("Execution failed");
        }

        String expected = loadExpectedResult(resultFile);

        assertEquals(expected, result);
    }

    private static Stream<Arguments> provideQueryTestData() {
        return Stream.of(
                Arguments.of("SELECT * FROM uwa", "fullScan.txt"),
                Arguments.of("SELECT * FROM uwa WHERE airTemp >= 200", "indexScan.txt"),
                Arguments.of("SELECT * FROM uwa WHERE airTemp > 200", "indexScan2.txt"),
                Arguments.of("SELECT * FROM uwa WHERE airTemp >= 200 AND airTemp <= 600", "indexScan3.txt"),
                Arguments.of("SELECT * FROM uwa WHERE airTemp <= 600", "indexScan4.txt"),
                Arguments.of("SELECT * FROM uwa WHERE airTemp < 600", "indexScan5.txt"),
                Arguments.of("SELECT * FROM uwa WHERE airTemp = 600", "indexScan6.txt"),
                Arguments.of("SELECT * FROM uwa WHERE airTemp != 600", "indexScan7.txt"),
                Arguments.of("SELECT * FROM uwa WHERE airTemp <> 600", "indexScan7.txt"),
                Arguments.of("SELECT id, airTemp, airPres FROM uwa", "projectionConsecutive.txt"),
                Arguments.of("SELECT id, windSpeed FROM uwa", "projectionNonConsecutive.txt"),
                Arguments.of("SELECT * FROM uwa WHERE windSpeed > 200", "selectionSingleGreater.txt"),
                Arguments.of("SELECT * FROM uwa WHERE windSpeed >= 200", "selectionSingleGreaterEqual.txt"),
                Arguments.of("SELECT * FROM uwa WHERE windSpeed < 200", "selectionSingleLess.txt"),
                Arguments.of("SELECT * FROM uwa WHERE windSpeed <= 200", "selectionSingleLessEqual.txt"),
                Arguments.of("SELECT * FROM uwa WHERE windSpeed = 200", "selectionSingleEqual.txt"),
                Arguments.of("SELECT * FROM uwa WHERE windSpeed != 200", "selectionSingleNotEqual.txt"),
                Arguments.of("SELECT * FROM uwa WHERE windSpeed <> 200", "selectionSingleNotEqual.txt"),
                Arguments.of("SELECT * FROM uwa WHERE windSpeed > 200 AND airPres >= 100 AND airPres < 500", "selectionMultipleAnd.txt"),
                Arguments.of("SELECT id, airTemp FROM uwa WHERE id >= 1000000 AND airTemp < 500", "select1.txt"),
                Arguments.of("SELECT floor(id / 86400), min(airTemp), max(airTemp), avg(airTemp) FROM uwa GROUP BY floor(id / 86400) order by floor(id / 86400)", "groupBy1.txt"),
                Arguments.of("SELECT floor(id / 86400) as \"Day\", avg(airTemp) as \"AvgTemp\", max(windspeed) as \"MaxWind\" FROM uwa GROUP BY \"Day\" HAVING max(windspeed) > 250", "having1.txt"),
                Arguments.of("SELECT min(airTemp) FROM uwa", "min.txt")
        );
    }

    private static String loadExpectedResult(String fileName) throws IOException {
        // Read text from file
        String filePath = "tests/TestOutputs/" + fileName;
        return new String(Files.readAllBytes(Paths.get(filePath))).replaceAll("\r\n", "\n");
    }
}
