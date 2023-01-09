/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.benchmark;

import java.lang.invoke.MethodHandle;

public class IntSum
{
    private IntSum() {}

    public static int sum(int a, int b)
    {
        return a + b;
    }

    public static int sum2(int a, int b)
    {
        return a + b;
    }

    public static int sum3(int a, int b)
    {
        return a + b;
    }

    public static int sum4(int a, int b)
    {
        return a + b;
    }

    public static int metaSum(MethodHandle sum, int first, int iterations)
            throws Throwable
    {
        int result = 0;
        for (int i = 0; i < iterations; ++i) {
            result += (int) sum.invokeExact(first, result);
        }
        return result;
    }
}
