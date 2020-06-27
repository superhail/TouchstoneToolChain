package ecnu.db.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author xuechao.lian
 */
public class CommonUtils {
    /**
     * 获取正则表达式的匹配
     * @param pattern
     * @param str
     * @return 成功的所有匹配，一个{@code List<String>}对应一个匹配的所有group
     */
    public static List<List<String>> matchPattern(Pattern pattern, String str) {
        Matcher matcher = pattern.matcher(str);
        List<List<String>> ret = new ArrayList<>();
        while (matcher.find()) {
            List<String> groups = new ArrayList<>();
            for (int i = 0; i <= matcher.groupCount(); i++) {
                groups.add(matcher.group(i));
            }
            ret.add(groups);
        }

        return ret;
    }
}
