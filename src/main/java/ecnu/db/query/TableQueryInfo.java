package ecnu.db.query;

import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.HashSet;

/**
 * @author wangqingshuai
 */
public class TableQueryInfo {
    String tableName;
    Pair<String, BigDecimal> filterInfo;
    /**
     * 外键join信息和过滤比例，其中外键join信息的表示为
     * columnName,referencingTable.columnName
     */
    HashMap<String, BigDecimal> fkJoinInfos;


    public TableQueryInfo(String tableName) {
        this.tableName = tableName;
    }

    public void setFilterCondition(String condition) {
        filterInfo = new MutablePair<>(condition, null);
    }

    public void setFilterProbability(BigDecimal probability) throws Exception {
        if (filterInfo != null) {
            filterInfo.setValue(probability);
        } else {
            throw new Exception("在query中没有检测到" + tableName + "的where过滤条件");
        }
    }

    public void setFkJoinInfos(HashSet<String> referencingTableNames) {
        fkJoinInfos = new HashMap<>();
        for (String referencingTableName : referencingTableNames) {
            fkJoinInfos.put(referencingTableName, null);
        }
    }

    public void setFkJoinInfoProbability(String referencingTableName, BigDecimal probability) {
        for (String fkJoinInfo : fkJoinInfos.keySet()) {
            if (referencingTableName.equals(fkJoinInfo.split(",")[1].split(".")[0])) {
                fkJoinInfos.put(fkJoinInfo, probability);
                return;
            }
        }
    }
}
