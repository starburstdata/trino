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
package io.trino.parquet.reader;

import com.google.common.collect.AbstractIterator;
import io.trino.parquet.DataPage;
import io.trino.parquet.DataPageV1;
import io.trino.parquet.DataPageV2;
import io.trino.parquet.DictionaryPage;
import io.trino.parquet.ParquetCorruptionException;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.format.DataPageHeader;
import org.apache.parquet.format.DataPageHeaderV2;
import org.apache.parquet.format.DictionaryPageHeader;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.Util;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Optional;
import java.util.OptionalLong;

import static io.trino.parquet.ParquetTypeUtils.getParquetEncoding;
import static java.util.Objects.requireNonNull;
import static org.apache.parquet.format.PageType.DICTIONARY_PAGE;

public class ParquetColumnChunkIterator
        extends AbstractIterator<DataPage>
{
    private final Optional<String> fileCreatedBy;
    private final ColumnChunkDescriptor descriptor;
    private ChunkedInputStream input;
    private final OffsetIndex offsetIndex;

    private PageHeader firstPageHeader;
    private DictionaryPage dictionaryPage;
    private long valueCount;
    private int dataPageCount;

    public ParquetColumnChunkIterator(
            Optional<String> fileCreatedBy,
            ColumnChunkDescriptor descriptor,
            ChunkedInputStream input,
            OffsetIndex offsetIndex)
            throws IOException
    {
        this.fileCreatedBy = requireNonNull(fileCreatedBy, "fileCreatedBy is null");
        this.descriptor = requireNonNull(descriptor, "descriptor is null");
        this.input = requireNonNull(input, "input is null");
        this.offsetIndex = offsetIndex;
        if (hasMorePages(valueCount, dataPageCount)) {
            PageHeader pageHeader = readPageHeader();
            if (pageHeader.type.equals(DICTIONARY_PAGE)) {
                dictionaryPage = readDictionaryPage(pageHeader, pageHeader.getUncompressed_page_size(), pageHeader.getCompressed_page_size());
            }
            else {
                firstPageHeader = pageHeader;
            }
        }
    }

    @Nullable
    @Override
    protected DataPage computeNext()
    {
        if (!hasMorePages(valueCount, dataPageCount)) {
            return endOfData();
        }

        try {
            PageHeader pageHeader = readPageHeader();
            int uncompressedPageSize = pageHeader.getUncompressed_page_size();
            int compressedPageSize = pageHeader.getCompressed_page_size();
            DataPage result = null;
            switch (pageHeader.type) {
                case DICTIONARY_PAGE:
                    throw new ParquetCorruptionException("%s has dictionary page at not first position in column chunk", descriptor.getColumnDescriptor());
                case DATA_PAGE:
                    result = readDataPageV1(pageHeader, uncompressedPageSize, compressedPageSize, getFirstRowIndex(dataPageCount, offsetIndex));
                    ++dataPageCount;
                    break;
                case DATA_PAGE_V2:
                    result = readDataPageV2(pageHeader, uncompressedPageSize, compressedPageSize, getFirstRowIndex(dataPageCount, offsetIndex));
                    ++dataPageCount;
                    break;
                default:
                    input.skip(compressedPageSize);
                    break;
            }
            return result;
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected PageHeader readPageHeader()
            throws IOException
    {
        if (firstPageHeader != null) {
            PageHeader pageHeader = firstPageHeader;
            firstPageHeader = null;
            return pageHeader;
        }
        return Util.readPageHeader(input);
    }

    private boolean hasMorePages(long valuesCountReadSoFar, int dataPageCountReadSoFar)
    {
        if (offsetIndex == null) {
            return valuesCountReadSoFar < descriptor.getColumnChunkMetaData().getValueCount();
        }
        return dataPageCountReadSoFar < offsetIndex.getPageCount();
    }

    private DictionaryPage readDictionaryPage(PageHeader pageHeader, int uncompressedPageSize, int compressedPageSize)
    {
        DictionaryPageHeader dicHeader = pageHeader.getDictionary_page_header();
        return new DictionaryPage(
                input.getSlice(compressedPageSize),
                uncompressedPageSize,
                dicHeader.getNum_values(),
                getParquetEncoding(Encoding.valueOf(dicHeader.getEncoding().name())));
    }

    private DataPageV1 readDataPageV1(
            PageHeader pageHeader,
            int uncompressedPageSize,
            int compressedPageSize,
            OptionalLong firstRowIndex)
    {
        DataPageHeader dataHeaderV1 = pageHeader.getData_page_header();
        valueCount += dataHeaderV1.getNum_values();
        return new DataPageV1(
                input.getSlice(compressedPageSize),
                dataHeaderV1.getNum_values(),
                uncompressedPageSize,
                firstRowIndex,
                getParquetEncoding(Encoding.valueOf(dataHeaderV1.getRepetition_level_encoding().name())),
                getParquetEncoding(Encoding.valueOf(dataHeaderV1.getDefinition_level_encoding().name())),
                getParquetEncoding(Encoding.valueOf(dataHeaderV1.getEncoding().name())));
    }

    private DataPageV2 readDataPageV2(
            PageHeader pageHeader,
            int uncompressedPageSize,
            int compressedPageSize,
            OptionalLong firstRowIndex)
    {
        DataPageHeaderV2 dataHeaderV2 = pageHeader.getData_page_header_v2();
        int dataSize = compressedPageSize - dataHeaderV2.getRepetition_levels_byte_length() - dataHeaderV2.getDefinition_levels_byte_length();
        valueCount += dataHeaderV2.getNum_values();
        return new DataPageV2(
                dataHeaderV2.getNum_rows(),
                dataHeaderV2.getNum_nulls(),
                dataHeaderV2.getNum_values(),
                input.getSlice(dataHeaderV2.getRepetition_levels_byte_length()),
                input.getSlice(dataHeaderV2.getDefinition_levels_byte_length()),
                getParquetEncoding(Encoding.valueOf(dataHeaderV2.getEncoding().name())),
                input.getSlice(dataSize),
                uncompressedPageSize,
                firstRowIndex,
                MetadataReader.readStats(
                        fileCreatedBy,
                        Optional.ofNullable(dataHeaderV2.getStatistics()),
                        descriptor.getColumnDescriptor().getPrimitiveType()),
                dataHeaderV2.isIs_compressed());
    }

    private static OptionalLong getFirstRowIndex(int pageIndex, OffsetIndex offsetIndex)
    {
        return offsetIndex == null ? OptionalLong.empty() : OptionalLong.of(offsetIndex.getFirstRowIndex(pageIndex));
    }

    @Nullable
    public DictionaryPage getDictionaryPage()
    {
        return dictionaryPage;
    }
}
