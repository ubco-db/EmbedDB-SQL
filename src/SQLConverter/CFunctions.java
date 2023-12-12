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
