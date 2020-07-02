package ecnu.db.schema.column;

import com.alibaba.fastjson.serializer.JSONSerializer;
import com.alibaba.fastjson.serializer.ObjectSerializer;

import java.lang.reflect.Type;

public class ColumnSerializer implements ObjectSerializer {
    @Override
    public void write(JSONSerializer jsonSerializer, Object object, Object filedName, Type filedType, int features) {
        AbstractColumn column = (AbstractColumn) object;
        switch (column.getColumnType()) {
            case INTEGER:
                jsonSerializer.write(column);
                break;
            case BOOL:
                jsonSerializer.write(column);
                break;
            case DECIMAL:
                jsonSerializer.write(column);
                break;
            case VARCHAR:
                jsonSerializer.write(column);
                break;
            case DATETIME:
                jsonSerializer.write(column);
                break;
        }
    }
}

