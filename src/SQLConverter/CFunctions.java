/**
 * @file		CFunctions.java
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

package SQLConverter;

public class CFunctions {
    public static String floor() {
        return """
                int embedDBFloor(double x) {
                    int xi = (int)x;
                    return x < xi ? xi - 1 : xi;
                }
                """;
    }

    public static String ceil() {
        return """
                int embedDBCeil(double x) {
                    int xi = (int)x;
                    return x > xi ? xi + 1 : xi;
                }
                """;
    }

    public static String round() {
        return """
                int embedDBRound(double x) {
                    x += 0.5;
                    int xi = (int)x;
                    return x < xi ? xi - 1 : xi;
                }
                """;
    }

    public static String abs() {
        return """
                double embedDBAbs(double x) {
                    return x < 0 ? -x : x;
                }
                """;
    }
}
