package io.trino.operator;

import io.airlift.slice.Slices;
import io.trino.SessionTestUtils;
import io.trino.operator.scalar.DateTimeFunctions;
import org.testng.annotations.Test;

public class TestDateParse
{

    @Test
    public void test()
    {
        long parsed = DateTimeFunctions.dateParse(SessionTestUtils.TEST_SESSION.toConnectorSession(), Slices.utf8Slice("1970-01-01"), Slices.utf8Slice("%Y-%m-%d"));
        System.out.println(parsed);

        parsed = DateTimeFunctions.dateParse(SessionTestUtils.TEST_SESSION.toConnectorSession(), Slices.utf8Slice("-1-01-01"), Slices.utf8Slice("%Y-%m-%d"));
        System.out.println(parsed);
    }
}
