package simpledb.common;

public class CommonUtil {
    public static boolean isStringEqual(String s1,String s2){
        if (s1 == s2) {
            return true;
        }
        if (s1 == null || s2 == null) {
            return false;
        }
        return s1.equals(s2);
    }
}
