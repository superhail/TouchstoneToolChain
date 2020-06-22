package ecnu.db.analyzer.statical;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.visitor.SchemaStatVisitor;
import com.alibaba.druid.stat.TableStat;

import java.util.HashSet;
import java.util.List;

/**
 * @author wangqingshuai
 */
public class QueryTableName {
    public static HashSet<String> getTableName(String sql, String dbType) {
        List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, dbType);
        SQLStatement stmt = stmtList.get(0);

        SchemaStatVisitor statVisitor = SQLUtils.createSchemaStatVisitor(dbType);
        stmt.accept(statVisitor);
        HashSet<String> tableName = new HashSet<>();
        for (TableStat.Name name : statVisitor.getTables().keySet()) {
            tableName.add(name.getName().toLowerCase());
        }
        return tableName;
    }
}
