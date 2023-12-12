/**
 * @file		TestSQLConverter.java
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
