package SQLConverter;

import org.hsqldb.*;
import org.hsqldb.jdbc.JDBCConnection;
import org.hsqldb.jdbc.JDBCStatement;
import org.hsqldb.lib.HsqlArrayList;
import org.hsqldb.lib.OrderedHashMap;
import org.hsqldb.types.NumberType;
import org.hsqldb.types.Type;

import java.io.Closeable;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.stream.IntStream;

@SuppressWarnings({"SqlSourceToSinkFlow", "unchecked"})
public class SQLConverter implements Closeable {
    private final JDBCConnection conn;
    private final Statement stmt;

    public SQLConverter() throws SQLException {
        conn = (JDBCConnection) DriverManager.getConnection("jdbc:hsqldb:mem:test", "SA", "");
        stmt = conn.createStatement();
        stmt.execute("DROP SCHEMA PUBLIC CASCADE");
    }

    public SQLConverter(String... ddl) throws SQLException {
        this();
        for (String s : ddl)
            executeDDL(s);
    }

    @Override
    public void close() throws IOException {
        try {
            conn.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void executeDDL(String ddl) throws SQLException {
        stmt.executeUpdate(ddl);
    }

    public String toCCode(String query) throws RuntimeException {
        JDBCStatement jStmt = (JDBCStatement) stmt;
        HsqlArrayList<StatementCommand> compiledStatements;
        try {
            compiledStatements = jStmt.compileQuery("EXPLAIN PLAN FOR " + query);
        } catch (SQLException e) {
            throw new RuntimeException(e.getMessage());
        }

        if (compiledStatements == null || compiledStatements.isEmpty()) {
            throw new RuntimeException("No statement could be parsed");
        } else if (compiledStatements.size() > 1) {
            System.err.println("Only one statement can be compiled. Proceeding with the first statement");
        }

        StatementCommand explainCommand = compiledStatements.get(0);
        if (!(explainCommand.arguments[0] instanceof StatementQuery sq)) {
            throw new RuntimeException("Query did not compile correctly");
        }

        // Get C code
        if (sq.queryExpression instanceof QuerySpecification) {
            return toCCode((QuerySpecification) sq.queryExpression);
        } else {
            throw new RuntimeException("Unknown query format error");
        }
    }

    @SuppressWarnings({"MismatchedQueryAndUpdateOfStringBuilder"})
    private String toCCode(QuerySpecification query) {
        StringBuilder operatorCode;
        StringBuilder queryCode = new StringBuilder();
        OrderedHashMap<String, String> cFunctions = new OrderedHashMap<>();

        String indent = "";

        if (query.getRangeVariables().length != 1) {
            return "System cannot handle queries involving more than one table";
        }

        OperatorReturn r = getOperatorCode(query, cFunctions);
        if (!r.success) return r.errorMsg;

        operatorCode = r.operatorCode;
        Schema outputSchema = r.outputSchema;
        Schema tableSchema = r.tableSchema;

        // Create function header
        queryCode.append(indent).append("void execOperator(embedDBState* state) {\n");
        indent += "    ";

        // Init an iterator and list of allocated values
        queryCode.append(indent).append("void** allocatedValues;\n");

        // Create operator with function call
        queryCode.append(indent).append("embedDBOperator* op = createOperator(state, &allocatedValues);\n");

        // Get record buffer
        queryCode.append(indent).append("void* recordBuffer = op->recordBuffer;\n");

        // Create column pointers
        for (Schema.Column column : outputSchema) {
            int offset = tableSchema.getColOffset(column.name);
            queryCode.append(indent).append(column.dataType).append("* ").append(column.name).append(" = (").append(column.dataType).append("*)((int8_t*)recordBuffer + ").append(offset).append(");\n");
        }

        // Create loop that goes over iterator
        queryCode.append("\n").append(indent).append("// Print as csv\n");
        queryCode.append(indent).append("while (exec(op)) {\n");
        indent += "    ";

        // Process each record
        queryCode.append(indent).append("printf(\"");
        for (String type : outputSchema.colDataTypes()) {
            if (type.contains("int")) {
                queryCode.append("%d,");
            } else {
                queryCode.append("%f,");
            }
        }
        queryCode.delete(queryCode.length() - 1, queryCode.length());  // Remove last comma
        queryCode.append("\\n\", ");
        outputSchema.colNames().forEach(colName -> queryCode.append("*").append(colName).append(", "));
        queryCode.delete(queryCode.length() - 2, queryCode.length());  // Remove last comma
        queryCode.append(");\n");

        // Close loop that goes over iterator
        indent = indent.substring(0, indent.length() - 4);
        queryCode.append(indent).append("}\n");
        queryCode.append(indent).append("printf(\"\\n\");\n\n");

        // Close top level operator
        queryCode.append(indent).append("op->close(op);\n");

        // Free top level operator
        queryCode.append(indent).append("embedDBFreeOperatorRecursive(&op);\n");
        queryCode.append(indent).append("recordBuffer = NULL;\n");
        queryCode.append(indent).append("for (int i = 0; i < ").append(r.numVarsToFree).append("; i++) {\n");
        indent += "    ";
        queryCode.append(indent).append("free(allocatedValues[i]);\n");
        indent = indent.substring(0, indent.length() - 4);
        queryCode.append(indent).append("}\n");
        queryCode.append(indent).append("free(allocatedValues);\n");

        // Close function
        //noinspection ConstantValue
        indent = indent.substring(0, indent.length() - 4);
        queryCode.append(indent).append("}\n");

        // Build function code
        StringBuilder functionCode = new StringBuilder();
        String[] valuesToArray = cFunctions.valuesToArray(new String[cFunctions.size()]);
        for (int i = 0, valuesToArrayLength = valuesToArray.length; i < valuesToArrayLength; i++) {
            String func = valuesToArray[i];
            if (i != 0) {
                functionCode.append('\n');
            }
            functionCode.append(func);
        }

        // Put all parts together
        String output = "";
        if (!functionCode.isEmpty()) {
            output += functionCode + "\n";
        }
        if (!operatorCode.isEmpty()) {
            output += operatorCode + "\n";
        }
        if (!queryCode.isEmpty()) {
            output += queryCode;
        }
        return output;
    }

    private static class OperatorReturn {
        boolean success;
        String errorMsg;
        StringBuilder operatorCode;
        Schema outputSchema;
        Schema tableSchema;
        int numVarsToFree;

        public OperatorReturn(StringBuilder iteratorCode, Schema outputSchema, Schema tableSchema, int numVarsToFree) {
            this.success = true;
            this.operatorCode = iteratorCode;
            this.outputSchema = outputSchema;
            this.tableSchema = tableSchema;
            this.numVarsToFree = numVarsToFree;
        }

        public OperatorReturn(String errorMsg) {
            this.success = false;
            this.errorMsg = errorMsg;
        }
    }

    private OperatorReturn getOperatorCode(QuerySpecification query, OrderedHashMap<String, String> cFunctions) {
        RangeVariable rv = query.getRangeVariables()[0];

        if (rv.isLeftJoin || rv.isRightJoin) {
            return new OperatorReturn("Left and right joins are not supported");
        }

        StringBuilder operatorCode = new StringBuilder();
        ArrayList<String> freeVars = new ArrayList<>();
        ArrayList<String> localFreeVars = new ArrayList<>();
        String indent = "";

        // Get size of record/column schema
        Schema outputSchema = new Schema(query);
        Schema tableSchema = new Schema(rv.getTable());

        // Add function header
        operatorCode.append(indent).append("embedDBOperator* createOperator(embedDBState* state, void*** allocatedValues) {\n");
        indent += "    ";

        // Find all selections and sort them into index and non-index conditions
        ArrayList<LogicalSelection> indexSelections = new ArrayList<>();
        ArrayList<LogicalSelection> nonIndexSelections = new ArrayList<>();
        int[] columnIndexes = rv.getTable().bestIndexForColumn;
        for (LogicalSelection ls : findAllSelections(rv)) {
            if (columnIndexes[ls.colNum] >= 0) {
                indexSelections.add(ls);
            } else {
                nonIndexSelections.add(ls);
            }
        }

        // Find group by
        ArithmeticExpression groupByExpression = null;
        if (query.isAggregated && !query.isGrouped) {
            groupByExpression = new ArithmeticExpression(OpTypes.VALUE, null, null, 1);
            query.isGrouped = true;
            cFunctions.put("groupFunction", """
                    int8_t groupFunction(const void* lastRecord, const void* record) {
                        return 1;
                    }
                    """);
        } else if (query.isGrouped) {
            if (query.groupSet.groupExpressions.length > 1) {
                return new OperatorReturn("Grouping by multiple expressions is not currently supported");
            }

            // Parse expression
            Expression groupExpression = query.groupSet.groupExpressions[0];
            groupByExpression = getGroupBy(groupExpression, cFunctions);

            // Create group function
            StringBuilder functionCode = new StringBuilder();
            functionCode.append("int8_t groupFunction(const void* lastRecord, const void* record) {\n");
            String functionIndent = "    ";
            int col = groupByExpression.getCol();
            Schema.Column column = tableSchema.getColumn(col);
            String dataType = column.dataType;
            int offset = tableSchema.getColOffset(col);
            String formattedExpression = groupByExpression.toFormattedString();
            functionCode.append(functionIndent).append(dataType).append(" lastValue = *((").append(dataType).append("*)((int8_t*)lastRecord + ").append(offset).append("));\n");
            functionCode.append(functionIndent).append(dataType).append(" value = *((").append(dataType).append("*)((int8_t*)record + ").append(offset).append("));\n");
            functionCode.append(functionIndent).append("return ").append(String.format(formattedExpression, "lastValue")).append(" == ").append(String.format(formattedExpression, "value")).append(";\n");
            functionCode.append("}\n");
            cFunctions.put("groupFunction", functionCode.toString());
        }

        /*
         * Parse index selections into an embedDBIterator
         */
        boolean minKeyIsSet = false, maxKeyIsSet = false, minDataIsSet = false, maxDataIsSet = false;

        // Sort index selections by column number, so we have nice code
        indexSelections.sort(Comparator.comparingInt(o -> o.colNum));

        // Parse
        for (LogicalSelection ls : indexSelections) {
            boolean isKey = columnIndexes[ls.colNum] == 0;
            String dataTypeString = tableSchema.getColumn(ls.colNum).dataType;

            // Determine if this is a min or max value
            boolean skip = false;
            boolean isMin = false, isMax = false;
            switch (ls.type) {
                case OpTypes.GREATER_EQUAL -> isMin = true;
                case OpTypes.GREATER -> {
                    ls.value++;
                    isMin = true;
                }
                case OpTypes.SMALLER_EQUAL -> isMax = true;
                case OpTypes.SMALLER -> {
                    ls.value--;
                    isMax = true;
                }
                case OpTypes.EQUAL -> isMin = isMax = true;
                case OpTypes.NOT_EQUAL -> {
                    nonIndexSelections.add(ls);
                    skip = true;
                }
                default -> throw new RuntimeException("Unknown type");
            }
            if (skip) continue;

            // Allocate memory for value and set value
            if (isMin) {
                String valueVariableName = isKey ? "minKey" : "minData";
                operatorCode.append(indent).append(dataTypeString).append("* ").append(valueVariableName).append(" = (").append(dataTypeString).append("*)malloc(").append(tableSchema.getColSize(ls.colNum)).append(");\n");
                freeVars.add(valueVariableName);
                operatorCode.append(indent).append("*").append(valueVariableName).append(" = ").append(ls.value).append(";\n");
            }
            if (isMax) {
                String valueVariableName = isKey ? "maxKey" : "maxData";
                operatorCode.append(indent).append(dataTypeString).append("* ").append(valueVariableName).append(" = (").append(dataTypeString).append("*)malloc(").append(tableSchema.getColSize(ls.colNum)).append(");\n");
                freeVars.add(valueVariableName);
                operatorCode.append(indent).append("*").append(valueVariableName).append(" = ").append(ls.value).append(";\n");
            }

            // Flag that min or max value is set
            if (isKey) {
                if (isMin) {
                    minKeyIsSet = true;
                }
                if (isMax) {
                    maxKeyIsSet = true;
                }
            } else {
                if (isMin) {
                    minDataIsSet = true;
                }
                if (isMax) {
                    maxDataIsSet = true;
                }
            }
        }

        // Set up iterator
        operatorCode.append(indent).append("embedDBIterator* it = (embedDBIterator*)malloc(sizeof(embedDBIterator));\n");
        freeVars.add("it");
        if (minKeyIsSet) {
            operatorCode.append(indent).append("it->minKey = ").append("minKey").append(";\n");
        } else {
            operatorCode.append(indent).append("it->minKey = NULL;\n");
        }
        if (maxKeyIsSet) {
            operatorCode.append(indent).append("it->maxKey = ").append("maxKey").append(";\n");
        } else {
            operatorCode.append(indent).append("it->maxKey = NULL;\n");
        }
        if (minDataIsSet) {
            operatorCode.append(indent).append("it->minData = ").append("minData").append(";\n");
        } else {
            operatorCode.append(indent).append("it->minData = NULL;\n");
        }
        if (maxDataIsSet) {
            operatorCode.append(indent).append("it->maxData = ").append("maxData").append(";\n");
        } else {
            operatorCode.append(indent).append("it->maxData = NULL;\n");
        }
        operatorCode.append(indent).append("embedDBInitIterator(state, it);\n\n");

        /*
         * Parse non-index selections into embedDBOperators
         */
        String topLevelOperator;

        // Create schema struct
        operatorCode.append(indent).append("uint8_t numCols = ").append(tableSchema.getNumCols()).append(";\n");
        operatorCode.append(indent).append("int8_t colSizes[] = {");
        tableSchema.forEach(column -> operatorCode.append(column.size).append(", "));
        operatorCode.delete(operatorCode.length() - 2, operatorCode.length());  // Remove last comma
        operatorCode.append("};\n");
        operatorCode.append(indent).append("int8_t colSignedness[] = {");
        for (int j = 0; j < tableSchema.getNumCols(); j++) {
            if (j == 0) {
                operatorCode.append("embedDB_COLUMN_UNSIGNED");
            } else {
                operatorCode.append("embedDB_COLUMN_SIGNED");
            }
            if (j < tableSchema.getNumCols() - 1) {
                operatorCode.append(", ");
            }
        }
        operatorCode.append("};\n");
        operatorCode.append(indent).append("embedDBSchema* schema = embedDBCreateSchema(numCols, colSizes, colSignedness);\n");

        // Create table scan operator
        operatorCode.append(indent).append("embedDBOperator* scanOp = createTableScanOperator(state, it, schema);\n");
        topLevelOperator = "scanOp";

        // Turn each condition into a selection operator
        for (LogicalSelection ls : nonIndexSelections) {
            String type = switch (ls.type) {
                case OpTypes.GREATER_EQUAL -> "SELECT_GTE";
                case OpTypes.GREATER -> "SELECT_GT";
                case OpTypes.SMALLER -> "SELECT_LT";
                case OpTypes.SMALLER_EQUAL -> "SELECT_LTE";
                case OpTypes.NOT_EQUAL -> "SELECT_NEQ";
                case OpTypes.EQUAL -> "SELECT_EQ";
                default -> throw new RuntimeException("Unknown type");
            };
            Schema.Column column = tableSchema.getColumn(ls.colNum);
            String colName = column.name;
            String colNameWithType = type.substring(7) + colName;
            String selectionVariableName = "select" + colNameWithType;
            String dataType = column.dataType;

            String valueVariableName = "selVal" + colNameWithType;

            // Allocate memory for value and set value
            operatorCode.append(indent).append(dataType).append("* ").append(valueVariableName).append(" = (").append(dataType).append("*)malloc(").append(column.size).append(");\n");
            freeVars.add(valueVariableName);
            operatorCode.append(indent).append("*").append(valueVariableName).append(" = ").append(ls.value).append(";\n");

            // Create operator
            operatorCode.append(indent).append("embedDBOperator* ").append(selectionVariableName).append(" = createSelectionOperator(").append(topLevelOperator).append(", ").append(ls.colNum).append(", ").append(type).append(", selVal").append(colNameWithType).append(");\n");
            topLevelOperator = selectionVariableName;
        }

        if (query.havingColumnCount > 1) {
            throw new RuntimeException("Multiple HAVING not allowed");
        }
        boolean hasHaving = query.havingColumnCount == 1;
        int havingColumnNum = query.indexLimitVisible;
        if (hasHaving && !(query.exprColumns[query.indexStartHaving] instanceof ExpressionLogical)) {
            throw new RuntimeException("HAVING clause must be a logical expression");
        }

        // Create group by operator
        if (query.isGrouped) {
            ArrayList<String> aggFuncs = new ArrayList<>();
            ArrayList<Expression> usedColumns = new ArrayList<>();
            Schema newSchema = new Schema();

            IntStream intStream = IntStream.range(0, query.indexLimitVisible);
            if (hasHaving) intStream = IntStream.concat(intStream, IntStream.of(query.indexStartHaving));
            int[] array = intStream.toArray();
            for (int j = 0; j < array.length; j++) {
                int i = array[j];
                Expression columnExpression = query.exprColumns[i];

                // This is a logical expression. Extract the "column" from it since this is a value that needs to be calculated
                if (hasHaving && j == array.length - 1) {
                    if (columnExpression.getLeftNode() instanceof ExpressionColumn)
                        columnExpression = columnExpression.getLeftNode();
                    else if (columnExpression.getRightNode() instanceof ExpressionColumn)
                        columnExpression = columnExpression.getRightNode();
                    else
                        throw new RuntimeException("HAVING expression is invalid");

                    // Check if this is a column we've already calculated
                    Expression referencedColumn = query.exprColumns[columnExpression.getColumnIndex()];
                    if (usedColumns.contains(referencedColumn)) {
                        havingColumnNum = usedColumns.indexOf(referencedColumn);
                        continue;
                    }
                }

                // Check if this column is simply pointing to a different column
                if (columnExpression.getType() == OpTypes.SIMPLE_COLUMN) {
                    int pointedToExpr = columnExpression.getColumnIndex();
                    columnExpression = query.exprColumns[pointedToExpr];
                }

                usedColumns.add(columnExpression);

                int type = columnExpression.getType();

                // Handle aggregated columns
                String colName = columnExpression.getAlias().replaceAll("\\s", "_");
                if (isSupportedAggregate(columnExpression)) {
                    String aggFuncName = null;
                    switch (type) {
                        case OpTypes.MAX, OpTypes.MIN -> {
                            int colNum = columnExpression.getLeftNode().getColumnIndex();
                            int colSize = tableSchema.getColSize(colNum);
                            if (colSize > 0) colSize *= -1;
                            aggFuncName = (type == OpTypes.MAX ? "MAX" : "MIN") + colName;

                            // Create aggregate function
                            operatorCode.append(indent).append("embedDBAggregateFunc* ").append(aggFuncName).append(" = create").append(type == OpTypes.MAX ? "Max" : "Min").append("Aggregate(").append(colNum).append(", ").append(colSize).append(");\n");
                            aggFuncs.add(aggFuncName);

                            newSchema.addColumn(colName, -colSize);
                        }
                        case OpTypes.COUNT -> {
                            aggFuncName = "counter" + i;
                            operatorCode.append(indent).append("embedDBAggregateFunc* ").append(aggFuncName).append(" = createCountAggregate();\n");
                            aggFuncs.add(aggFuncName);

                            newSchema.addColumn(colName, 4);
                        }
                        case OpTypes.SUM -> {
                            int colNum = columnExpression.getLeftNode().getColumnIndex();
                            aggFuncName = "sum" + i;
                            operatorCode.append(indent).append("embedDBAggregateFunc* ").append(aggFuncName).append(" = createSumAggregate(").append(colNum).append(");\n");
                            aggFuncs.add(aggFuncName);

                            newSchema.addColumn(colName, 8);
                        }
                        case OpTypes.AVG -> {
                            int colNum = columnExpression.getLeftNode().getColumnIndex();
                            aggFuncName = "avg" + i;
                            operatorCode.append(indent).append("embedDBAggregateFunc* ").append(aggFuncName).append(" = createAvgAggregate(").append(colNum).append(", 4);\n");
                            aggFuncs.add(aggFuncName);

                            newSchema.addColumn(colName, 4, true);

                            // Make output schema use float
                            Schema.Column outputColumn = outputSchema.getColumn(colName);
                            if (outputColumn != null)
                                outputColumn.dataType = "float";
                        }
                    }
                    if (aggFuncName != null)
                        localFreeVars.add(aggFuncName);
                } else {
                    // This has to be a non aggregated column, HSQL doesn't allow non-aggregated columns if they aren't in the GROUP BY, and we only allow one GROUP BY expression
                    // Thus we need to create a custom aggregateFunction that will only have a compute function
                    assert groupByExpression != null;  // Because will be set whenever query.isGrouped is true

                    /* Build custom function */
                    String functionName = "customAggregateFunc" + i;
                    StringBuilder functionCode = new StringBuilder();
                    functionCode.append("void ").append(functionName).append("(embedDBAggregateFunc* aggFunc, embedDBSchema* schema, void* recordBuffer, const void* lastRecord) {\n");
                    String functionIndent = "    ";
                    // Extract col value from lastRecord
                    int colNum = groupByExpression.getCol();
                    String dataType = tableSchema.getColumn(colNum).dataType;
                    int offset = tableSchema.getColOffset(colNum);
                    functionCode.append(functionIndent).append(dataType).append(" lastValue = *((").append(dataType).append("*)((int8_t*)lastRecord + ").append(offset).append("));\n");
                    functionCode.append(functionIndent).append(dataType).append(" calculatedValue = ").append(String.format(groupByExpression.toFormattedString(), "lastValue")).append(";\n");
                    // memcpy value into record
                    functionCode.append(functionIndent).append("memcpy((int8_t*)recordBuffer + getColOffsetFromSchema(schema, aggFunc->colNum), &calculatedValue, sizeof(").append(dataType).append("));\n");
                    functionCode.append("}\n");

                    cFunctions.put(functionName, functionCode.toString());

                    operatorCode.append(indent).append("embedDBAggregateFunc* group = (embedDBAggregateFunc*)calloc(1, sizeof(embedDBAggregateFunc));\n");
                    localFreeVars.add("group");
                    operatorCode.append(indent).append("group->compute = ").append(functionName).append(";\n");
                    int colSize = tableSchema.getColSize(colNum);
                    operatorCode.append(indent).append("group->colSize = ").append(colSize).append(";\n");

                    aggFuncs.add("group");

                    Schema.Column oldColumn = tableSchema.getColumn(colNum);
                    newSchema.addColumn(colName, oldColumn.size, groupByExpression.isFloat());
                }
            }

            // Create aggregate operator
            operatorCode.append(indent).append("embedDBAggregateFunc* aggFuncs = (embedDBAggregateFunc*)malloc(").append(aggFuncs.size()).append("*sizeof(embedDBAggregateFunc));\n");
            freeVars.add("aggFuncs");
            for (int i = 0; i < aggFuncs.size(); i++) {
                String aggFunc = aggFuncs.get(i);
                operatorCode.append(indent).append("aggFuncs[").append(i).append("] = *").append(aggFunc).append(";\n");
            }
            operatorCode.append(indent).append("embedDBOperator* aggOp = createAggregateOperator(").append(topLevelOperator).append(", groupFunction, aggFuncs, ").append(aggFuncs.size()).append(");\n");
            topLevelOperator = "aggOp";

            // Update schema since the aggregate operator completely redefines it
            tableSchema = newSchema;

            // Make sure the types of the output schema match
            for (int i = 0; i < outputSchema.getNumCols(); i++) {
                Schema.Column column = outputSchema.getColumn(i);
                Schema.Column newColumn = tableSchema.getColumn(column.name);
                if (newColumn != null) {
                    column.dataType = newColumn.dataType;
                }
            }
        }

        // Apply selection for HAVING
        if (query.havingColumnCount == 1) {
            Expression havingExpression = query.exprColumns[query.indexStartHaving];
            if (!(havingExpression instanceof ExpressionLogical)) {
                throw new RuntimeException("HAVING clause must be a logical expression");
            }

            Expression left = havingExpression.getLeftNode();
            Expression right = havingExpression.getRightNode();

            long value;
            if (left instanceof ExpressionColumn && right instanceof ExpressionValue) {
                value = getIntValue(right);
            } else if (left instanceof ExpressionValue && right instanceof ExpressionColumn) {
                value = getIntValue(left);
            } else {
                throw new RuntimeException("HAVING expression invalid");
            }

            // Allocate value
            String valueVariableName = "havingValue";
            String dataType = tableSchema.getColumn(havingColumnNum).dataType;
            operatorCode.append(indent).append(dataType).append("* ").append(valueVariableName).append(" = (").append(dataType).append("*)malloc(sizeof(").append(dataType).append("));\n");
            freeVars.add(valueVariableName);

            // Set value
            operatorCode.append(indent).append("*").append(valueVariableName).append(" = ").append(value).append(";\n");

            // Create selection operator
            String type = switch (havingExpression.getType()) {
                case OpTypes.GREATER_EQUAL -> "SELECT_GTE";
                case OpTypes.GREATER -> "SELECT_GT";
                case OpTypes.SMALLER -> "SELECT_LT";
                case OpTypes.SMALLER_EQUAL -> "SELECT_LTE";
                case OpTypes.NOT_EQUAL -> "SELECT_NEQ";
                case OpTypes.EQUAL -> "SELECT_EQ";
                default -> throw new RuntimeException("Unknown type");
            };
            operatorCode.append(indent).append("embedDBOperator* havingOp = createSelectionOperator(").append(topLevelOperator).append(", ").append(havingColumnNum).append(", ").append(type).append(", havingValue);\n");
            topLevelOperator = "havingOp";
        }

        // Init top level operator
        operatorCode.append(indent).append(topLevelOperator).append("->init(").append(topLevelOperator).append(");\n\n");

        // Free schema
        operatorCode.append(indent).append("embedDBFreeSchema(&schema);\n");

        // Free local vars
        for (String var : localFreeVars)
            operatorCode.append(indent).append("free(").append(var).append(");\n");
        operatorCode.append('\n');

        // Allocate array for vars that must be freed
        operatorCode.append(indent).append("*allocatedValues = (void**)malloc(").append(freeVars.size()).append(" * sizeof(void*));\n");
        for (int i = 0; i < freeVars.size(); i++) {
            operatorCode.append(indent).append("((void**)*allocatedValues)[").append(i).append("] = ").append(freeVars.get(i)).append(";\n");
        }
        operatorCode.append("\n");

        // Return operator
        operatorCode.append(indent).append("return ").append(topLevelOperator).append(";\n");

        // Close function
        //noinspection ConstantValue
        indent = indent.substring(0, indent.length() - 4);
        operatorCode.append(indent).append("}\n");

        // Return iterator code with other info
        return new OperatorReturn(operatorCode, outputSchema, tableSchema, freeVars.size());
    }

    private ArrayList<LogicalSelection> findAllSelections(RangeVariable rv) {
        // Use a set to avoid duplicates
        Set<RangeVariable.RangeVariableConditions> rvConditions = new HashSet<>();
        if (rv.joinConditions != null) rvConditions.addAll(Arrays.asList(rv.joinConditions));
        if (rv.whereConditions != null) rvConditions.addAll(Arrays.asList(rv.whereConditions));

        // Get all expressions from the conditions
        Set<Expression> expressions = new HashSet<>();
        for (RangeVariable.RangeVariableConditions rvc : rvConditions) {
            if (rvc.indexCond != null) expressions.addAll(Arrays.asList(rvc.indexCond));
            if (rvc.indexEndCond != null) expressions.addAll(Arrays.asList(rvc.indexEndCond));
            if (rvc.indexEndCondition != null) expressions.add(rvc.indexEndCondition);
            if (rvc.nonIndexCondition != null) expressions.add(rvc.nonIndexCondition);
        }

        // Parse all expressions
        Set<LogicalSelection> logicalSelections = new HashSet<>();
        for (Expression expr : expressions)
            if (expr != null) getLogicalSelections(expr, logicalSelections);

        return new ArrayList<>(logicalSelections);
    }

    private ArithmeticExpression getGroupBy(Expression expr, OrderedHashMap<String, String> cFunctions) {
        if (expr == null) {
            throw new RuntimeException("Expression cannot be null");
        }

        if (isSupportedArithmetic(expr)) {
            Expression left = expr.getLeftNode();
            Expression right = expr.getRightNode();

            if (left == null || right == null) {
                throw new RuntimeException("Expression cannot be null");
            }

            ArithmeticExpression leftArithmetic = getGroupBy(left, cFunctions);
            ArithmeticExpression rightArithmetic = getGroupBy(right, cFunctions);

            return new ArithmeticExpression(expr.getType(), leftArithmetic, rightArithmetic, 0);
        } else if (expr.getType() == OpTypes.COLUMN) {
            return new ArithmeticExpression(OpTypes.COLUMN, null, null, expr.getColumnIndex());
        } else if (expr.getType() == OpTypes.VALUE) {
            if (expr.valueData instanceof BigDecimal) {
                return new ArithmeticExpression(OpTypes.VALUE, null, null, getFloatValue(expr));
            } else {
                return new ArithmeticExpression(OpTypes.VALUE, null, null, (int) getIntValue(expr));
            }
        } else if (expr.getType() == OpTypes.SQL_FUNCTION && expr instanceof FunctionSQL functionSQL) {
            if (!isSupportedFunction(expr)) throw new RuntimeException(String.format("Function '%s' is not supported", functionSQL.name));

            // Add correct C function to code
            switch (functionSQL.funcType) {
                case FunctionSQL.FUNC_FLOOR -> cFunctions.put("embedDBFloor", CFunctions.floor());
                case FunctionSQL.FUNC_CEILING -> cFunctions.put("embedDBCeil", CFunctions.ceil());
                case FunctionSQL.FUNC_ABS -> cFunctions.put("embedDBAbs", CFunctions.abs());
                case FunctionCustom.FUNC_ROUND -> cFunctions.put("embedDBRound", CFunctions.round());
            }

            ArithmeticExpression leftArithmetic = getGroupBy(expr.getLeftNode(), cFunctions);
            return new ArithmeticExpression(OpTypes.SQL_FUNCTION, leftArithmetic, null, functionSQL.funcType);
        } else {
            throw new RuntimeException("Expression type not supported");
        }
    }

    static class ArithmeticExpression {
        public int type;
        public ArithmeticExpression left;
        public ArithmeticExpression right;
        public double value;
        private final boolean isFloat;

        public ArithmeticExpression(int type, ArithmeticExpression left, ArithmeticExpression right, int value) {
            this.type = type;
            this.left = left;
            this.right = right;
            this.value = value;
            this.isFloat = false;
        }

        public ArithmeticExpression(int type, ArithmeticExpression left, ArithmeticExpression right, double value) {
            this.type = type;
            this.left = left;
            this.right = right;
            this.value = value;
            isFloat = type == OpTypes.VALUE; // Only true if this expression is a float VALUE
        }

        @Override
        public String toString() {
            if (numCols() != 1) throw new RuntimeException("Expression must have exactly one column");
            return String.format(toFormattedString(), "col" + getCol());
        }

        public String toFormattedString() {
            if (type == OpTypes.VALUE) {
                return isFloat ? String.valueOf(value) : String.valueOf((int) value);
            } else if (type == OpTypes.COLUMN) {
                return "%s";
            } else if (type == OpTypes.SQL_FUNCTION) {
                return switch ((int) value) {
                    case FunctionSQL.FUNC_FLOOR -> "embedDBFloor";
                    case FunctionSQL.FUNC_CEILING -> "embedDBCeil";
                    case FunctionSQL.FUNC_ABS -> "embedDBAbs";
                    case FunctionCustom.FUNC_ROUND -> "embedDBRound";
                    default -> throw new RuntimeException("Unknown SQL function");
                } + "(" + left.toFormattedString() + ")";
            } else {
                return "(" + left.toFormattedString() + " " + switch (type) {
                    case OpTypes.ADD -> "+";
                    case OpTypes.SUBTRACT -> "-";
                    case OpTypes.MULTIPLY -> "*";
                    case OpTypes.DIVIDE -> "/";
                    default -> throw new RuntimeException("Unknown type");
                } + " " + right.toFormattedString() + ")";
            }
        }

        public int numCols() {
            if (type == OpTypes.COLUMN) {
                return 1;
            } else if (type == OpTypes.VALUE) {
                return 0;
            } else if (type == OpTypes.SQL_FUNCTION) {
                return left.numCols();
            } else {
                return left.numCols() + right.numCols();
            }
        }

        public int getCol() {
            if (type == OpTypes.COLUMN) {
                return (int) value;
            } else if (type == OpTypes.VALUE) {
                return -1;
            } else if (type == OpTypes.SQL_FUNCTION) {
                return left.getCol();
            } else {
                return Math.max(left.getCol(), right.getCol());
            }
        }

        public boolean isFloat() {
            if (type == OpTypes.VALUE) {
                return isFloat;
            } else if (type == OpTypes.COLUMN) {
                return false;
            } else if (type == OpTypes.SQL_FUNCTION) {
                return false;
            } else {
                return left.isFloat() || right.isFloat();
            }
        }
    }

    private boolean isSupportedArithmetic(Expression expr) {
        return switch (expr.getType()) {
            case OpTypes.ADD, OpTypes.SUBTRACT, OpTypes.MULTIPLY, OpTypes.DIVIDE -> true;
            default -> false;
        };
    }

    private boolean isSupportedAggregate(Expression expr) {
        return switch (expr.getType()) {
            case OpTypes.MAX, OpTypes.MIN, OpTypes.SUM, OpTypes.COUNT, OpTypes.AVG -> true;
            default -> false;
        };
    }

    private boolean isSupportedFunction(Expression expr) {
        if (expr.getType() == OpTypes.SQL_FUNCTION && expr instanceof FunctionSQL functionSQL)
            return switch (functionSQL.funcType) {
                case FunctionSQL.FUNC_FLOOR, FunctionSQL.FUNC_CEILING, FunctionSQL.FUNC_ABS, FunctionCustom.FUNC_ROUND ->
                        true;
                default -> false;
            };
        return false;
    }

    private boolean isSupportedValue(Expression expr) {
        if (expr.getType() == OpTypes.VALUE) return true;
        else return isSupportedFunction(expr);
    }

    private long getIntValue(Expression expr) {
        if (expr.valueData == null) {
            throw new RuntimeException("Value cannot be null");
        }
        if (expr.getType() != OpTypes.VALUE) {
            throw new RuntimeException("Expression must be a value");
        }

        if (expr.valueData instanceof Integer) {
            return ((Integer) expr.valueData).longValue();
        } else if (expr.valueData instanceof Long) {
            return (Long) expr.valueData;
        }
        throw new RuntimeException("Value in comparisons must be either INT or BIGINT");
    }

    private double getFloatValue(Expression expr) {
        if (expr.valueData == null) {
            throw new RuntimeException("Value cannot be null");
        }
        if (expr.getType() != OpTypes.VALUE) {
            throw new RuntimeException("Expression must be a value");
        }

        if (expr.valueData instanceof Integer) {
            return ((Integer) expr.valueData).doubleValue();
        } else if (expr.valueData instanceof Long) {
            return ((Long) expr.valueData).doubleValue();
        } else if (expr.valueData instanceof BigDecimal) {
            return ((BigDecimal) expr.valueData).doubleValue();
        }
        throw new RuntimeException("Value was not a number");
    }

    private void getLogicalSelections(Expression expr, Set<LogicalSelection> logicalSelections) {
        Expression left = expr.getLeftNode();
        Expression right = expr.getRightNode();
        if (expr.getType() == OpTypes.AND) {
            // Recursive case
            if (!(left instanceof ExpressionLogical) || !(right instanceof ExpressionLogical)) {
                throw new RuntimeException("Expressions on either side of AND must be logical");
            }
            getLogicalSelections(left, logicalSelections);
            getLogicalSelections(right, logicalSelections);
        } else if (expr.getType() == OpTypes.GREATER_EQUAL || expr.getType() == OpTypes.GREATER || expr.getType() == OpTypes.SMALLER || expr.getType() == OpTypes.SMALLER_EQUAL || expr.getType() == OpTypes.NOT_EQUAL || expr.getType() == OpTypes.EQUAL) {
            // Base case
            int columnIndex;
            long value;
            if (left.getType() == OpTypes.COLUMN && isSupportedValue(right)) {
                columnIndex = left.getColumnIndex();
                value = getIntValue(right);
            } else if (isSupportedValue(left) && right.getType() == OpTypes.COLUMN) {
                columnIndex = right.getColumnIndex();
                value = getIntValue(left);
            } else {
                throw new RuntimeException("Expressions must be logical with a column and a value");
            }
            logicalSelections.add(new LogicalSelection(columnIndex, expr.getType(), value));
        }
    }

    /**
     * Schema of a table or a range variable.
     * Maps column names to their size in bytes.
     * Also keeps track of the total record size and the number of columns.
     * Assumes that all columns are either INT or BIGINT.
     */
    @SuppressWarnings("unused")
    private static class Schema implements Iterable<Schema.Column> {
        private static class Column {
            String name;
            int size;
            String dataType;

            public Column(String name, int size, String dataType) {
                this.name = name;
                this.size = size;
                this.dataType = dataType;
            }
        }

        private final ArrayList<Column> columns = new ArrayList<>();

        Schema() {}

        Schema(QuerySpecification query) {
            // Output schema
            for (int i = 0; i < query.indexLimitVisible; i++) {
                Expression columnExpression = query.exprColumns[i];

                String colName = columnExpression.getAlias().replaceAll("\\s", "_");
                Type t = columnExpression.getDataType();
                if (t instanceof NumberType numberType) {
                    if (numberType.getNominalWidth() == 32) {
                        columns.add(new Column(colName, 4, i == 0 ? "uint32_t": "int32_t"));
                    } else if (numberType.getNominalWidth() == 64) {
                        columns.add(new Column(colName, 4, i == 0 ? "uint64_t": "int64_t"));
                    } else {
                        throw new RuntimeException("Column types must be either INT or BIGINT");
                    }
                }
            }
        }

        Schema(Table table) {
            for (int i = 0; i < table.getColumnCount(); i++) {
                ColumnSchema col = table.getColumn(i);
                String colName = col.getNameString();

                Type t = table.getColumn(i).getDataType();
                if (t instanceof NumberType numberType) {
                    if (numberType.getNominalWidth() == 32) {
                        columns.add(new Column(colName, 4, i == 0 ? "uint32_t" : "int32_t"));
                    } else if (numberType.getNominalWidth() == 64) {
                        columns.add(new Column(colName, 8, i == 0 ? "uint64_t" : "int64_t"));
                    } else {
                        throw new RuntimeException("Column types must be either INT or BIGINT");
                    }
                }
            }
        }

        @Override
        public Iterator<Column> iterator() {
            return columns.iterator();
        }

        public ArrayList<String> colNames() {
            ArrayList<String> names = new ArrayList<>(getNumCols());
            columns.forEach(column -> names.add(column.name));
            return names;
        }

        public ArrayList<Integer> colSizes() {
            ArrayList<Integer> sizes = new ArrayList<>(getNumCols());
            columns.forEach(column -> sizes.add(column.size));
            return sizes;
        }

        public ArrayList<String> colDataTypes() {
            ArrayList<String> types = new ArrayList<>(getNumCols());
            columns.forEach(column -> types.add(column.dataType));
            return types;
        }

        void addColumn(String name, int size, boolean isFloat) {
            String type;
            if (isFloat) {
                type = switch(size) {
                    case 4 -> "float";
                    case 8 -> "double";
                    default -> throw new RuntimeException("Cannot have a float column with size other than 4 or 8 bytes");
                };
            } else {
                type = "int" + (size * 8) + "_t";
            }
            columns.add(new Column(name, size, type));
        }

        void addColumn(String name, int size) {
            addColumn(name, size, false);
        }

        int getRecordSize() {
            int size = 0;
            for (Column c : columns)
                size += c.size;
            return size;
        }

        int getNumCols() {
            return columns.size();
        }

        int getColNum(String colName) {
            int i = 0;
            for (Column c : columns) {
                if (c.name.equals(colName)) {
                    return i;
                }
                i++;
            }
            return -1;
        }

        Column getColumn(String colName) {
            for (Column c : columns)
                if (c.name.equals(colName))
                    return c;
            return null;
        }

        Column getColumn(int colNum) {
            return columns.get(colNum);
        }

        int getColSize(String colName) {
            Column c = getColumn(colName);
            if (c == null)
                throw new IllegalArgumentException("Column " + colName + " doesn't exist");
            return c.size;
        }

        int getColSize(int colNum) {
            return getColumn(colNum).size;
        }

        int getColOffset(String colName) {
            int offset = 0;
            for (Column c : columns) {
                if (c.name.equals(colName)) {
                    break;
                }
                offset += c.size;
            }
            return offset;
        }

        int getColOffset(int colNum) {
            if (colNum >= columns.size())
                throw new IndexOutOfBoundsException();
            int offset = 0;
            for (int i = 0; i < colNum; i++) {
                offset += columns.get(i).size;
            }
            return offset;
        }
    }

    private static class LogicalSelection {
        public int colNum;
        public int type;
        public long value;

        public LogicalSelection(int colNum, int type, long value) {
            this.colNum = colNum;
            this.type = type;
            this.value = value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            LogicalSelection that = (LogicalSelection) o;
            return colNum == that.colNum && type == that.type && value == that.value;
        }

        @Override
        public int hashCode() {
            return Objects.hash(colNum, type, value);
        }
    }
}
