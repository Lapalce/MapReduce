import com.sun.xml.internal.ws.server.ServerRtException;

import java.io.*;
import java.security.Timestamp;

public class Test {
    public static void main(String[] args) {
        String ans = "20190102093318150";
        String input = ans;
        String year = input.substring(0, 4);
        String month = input.substring(4, 6);
        String day = input.substring(6, 8);
        String hour = input.substring(8, 10);
        String minute = input.substring(10, 12);
        String second = input.substring(12, 14);
        String millisecond = input.substring(14, 17);

        String time = String.format("%s-%s-%s %s:%s:%s.%s000", year, month, day, hour, minute, second, millisecond);
        System.out.println(time);
    }
}
