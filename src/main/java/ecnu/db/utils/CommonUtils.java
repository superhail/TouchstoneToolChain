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
     * @param planId
     * @return 成功的匹配
     */
    public static List<List<String>> matchPattern(Pattern pattern, String planId) {
        Matcher matcher = pattern.matcher(planId);
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
