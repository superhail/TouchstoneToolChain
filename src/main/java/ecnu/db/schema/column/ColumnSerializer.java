package ecnu.db.schema.column;

import com.alibaba.fastjson.serializer.JSONSerializer;
import com.alibaba.fastjson.serializer.ObjectSerializer;

import java.io.IOException;
import java.lang.reflect.Type;

public class ColumnSerializer implements ObjectSerializer {
    @Override
    public void write(JSONSerializer jsonSerializer, Object object, Object filedName, Type filedType, int features) throws IOException {
        AbstractColumn column = (AbstractColumn) object;
        switch (column.getColumnType()) {
            case INTEGER:
                jsonSerializer.write((IntColumn) column);
                break;
            case BOOL:
                jsonSerializer.write((BoolColumn) column);
                break;
            case DECIMAL:
                jsonSerializer.write((DecimalColumn) column);
                break;
            case VARCHAR:
                jsonSerializer.write((StringColumn) column);
                break;
            case DATETIME:
                jsonSerializer.write((DateColumn) column);
                break;
        }
    }
}
