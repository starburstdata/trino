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
package io.trino.operator;

import com.google.common.annotations.VisibleForTesting;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.type.Type;

import java.util.List;

public interface GroupByHash
{
    long getEstimatedSize();

    long getHashCollisions();

    double getExpectedHashCollisions();

    List<Type> getTypes();

    int getGroupCount();

    void appendValuesTo(int groupId, PageBuilder pageBuilder);

    Work<?> addPage(Page page);

    /**
     * The order of new group ids need to be the same as the order of incoming rows,
     * i.e. new group ids should be assigned in rows iteration order
     * Example:
     * rows:      A B C B D A E
     * group ids: 1 2 3 2 4 1 5
     */
    Work<GroupByIdBlock> getGroupIds(Page page);

    boolean contains(int position, Page page);

    default boolean contains(int position, Page page, long rawHash)
    {
        return contains(position, page);
    }

    long getRawHash(int groupyId);

    @VisibleForTesting
    int getCapacity();
}
