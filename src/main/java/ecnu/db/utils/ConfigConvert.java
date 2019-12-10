package ecnu.db.utils;

import ecnu.db.schema.column.ColumnType;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public class ConfigConvert {
    private static HashMap<HashSet<String>, ColumnType> typeConvert;

    public static void setTypeConvert(HashMap<HashSet<String>, ColumnType> typeConvert) {
        ConfigConvert.typeConvert = typeConvert;
    }

    public static ColumnType getColumnType(String readType) throws Exception {
        for (Map.Entry<HashSet<String>, ColumnType> hashSetColumnTypeEntry : typeConvert.entrySet()) {
            if (hashSetColumnTypeEntry.getKey().contains(readType)) {
                return hashSetColumnTypeEntry.getValue();
            }
        }
        throw new Exception("数据类型" + readType + "的匹配模版没有指定");
    }
}
