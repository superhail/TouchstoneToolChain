package ecnu.db.query.analyzer;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLSelect;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;
import com.alibaba.druid.sql.visitor.ParameterizedOutputVisitorUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * @author wangqingshuai
 * <p>
 * 使用阿里巴巴druid库 获取table信息
 */
public class QueryAliasParser {
    private ExportTableAliasVisitor statVisitor = new ExportTableAliasVisitor();

    public Map<String, String> getTableAlias(String sql, String dbType) {
        statVisitor.clear();
        SQLSelectStatement statement = (SQLSelectStatement) SQLUtils.parseStatements(sql, dbType).get(0);
        statement.accept(statVisitor);
        return statVisitor.getAliasMap();
    }

    private static class ExportTableAliasVisitor extends MySqlASTVisitorAdapter {
        private Map<String, String> aliasMap = new HashMap<>();

        public void clear() {
            aliasMap.clear();
        }

        @Override
        public boolean visit(SQLExprTableSource x) {
            if (x.getAlias() != null) {
                aliasMap.put(x.getAlias(), x.getName().getSimpleName());
            }
            return true;
        }

        public Map<String, String> getAliasMap() {
            if (aliasMap.size() == 0) {
                return null;
            } else {
                return aliasMap;
            }
        }
    }
}
