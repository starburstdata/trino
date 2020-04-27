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
package io.prestosql.elasticsearch;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.type.AbstractType;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.avg.AvgAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.max.MaxAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.min.MinAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.sum.SumAggregationBuilder;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.prestosql.elasticsearch.ElasticsearchMetadata.SUPPORTS_PREDICATES;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;
import static org.elasticsearch.search.aggregations.support.ValueType.STRING;

public class ElasticsearchAggregation
{
    //Use all lower case, because the table name is downcased before it gets to ElasticsearchMetadata
    private String bucketfield;
    private String buckettype;
    private Map<String, String> extrabucketattributes;

    private Map<String, SubAggregation> aggregations = new HashMap<>();

    @JsonCreator
    public ElasticsearchAggregation(@JsonProperty String bucketfield,
            @JsonProperty String buckettype,
            @JsonProperty Map<String, String> extrabucketattributes,
            @JsonProperty Map<String, Map<String, String>> aggregations)
    {
        this.bucketfield = bucketfield;
        this.buckettype = buckettype;
        this.extrabucketattributes = extrabucketattributes;
        if (aggregations != null) {
            aggregations.forEach((k, v) -> this.aggregations.put(k, new SubAggregation(v.get("field"), v.get("aggtype"))));
        }
    }

    @JsonProperty
    public String getBucketfield()
    {
        return bucketfield;
    }

    @JsonProperty
    public String getBuckettype()
    {
        return buckettype;
    }

    @JsonProperty
    public Map<String, String> getExtrabucketattributes()
    {
        return extrabucketattributes;
    }

    @JsonProperty
    public Map<String, SubAggregation> getAggregations()
    {
        return aggregations;
    }

    public List<ColumnMetadata> getColumnHandles()
    {
        ImmutableList.Builder<ColumnMetadata> result = ImmutableList.builder();

        result.add(ColumnMetadata.builder()
                .setName("key")
                .setType(buckettypeToType())
                .setHidden(false)
                .setProperties(ImmutableMap.of(SUPPORTS_PREDICATES, false))
                .build());
        result.add(ColumnMetadata.builder()
                .setName("doc_count")
                .setType(INTEGER)
                .setHidden(false)
                .setProperties(ImmutableMap.of(SUPPORTS_PREDICATES, false))
                .build());
        aggregations.forEach((fieldname, def) -> result.add(
                ColumnMetadata.builder()
                    .setName(fieldname)
                    .setType(DOUBLE)
                    .setHidden(false)
                    .setProperties(ImmutableMap.of(SUPPORTS_PREDICATES, false))
                    .build()));
        return result.build();
    }

    private AbstractType buckettypeToType()
    {
        switch (buckettype) {
            case "histogram":
                return INTEGER;
            case "terms":
                return VARCHAR;
            case "date_histogram":
                return DATE;
        }
        throw new PrestoException(NOT_SUPPORTED, "No type available for" + buckettype);
    }

    public AggregationBuilder buildAggregation()
    {
        AggregationBuilder builder;
        switch (buckettype) {
            case "histogram":
                builder = new HistogramAggregationBuilder(bucketfield + "_histogram")
                    .interval(Double.parseDouble(extrabucketattributes.get("interval")))
                    .field(bucketfield);
                break;
            case "terms":
                builder = new TermsAggregationBuilder(bucketfield + "terms", STRING)
                    .field(bucketfield);
                break;
            case "date_histogram":
                builder = new DateHistogramAggregationBuilder(bucketfield + "_date_histogram")
                    .dateHistogramInterval(new DateHistogramInterval(extrabucketattributes.get("interval")))
                    .field(bucketfield);
                break;
            default:
                throw new PrestoException(NOT_SUPPORTED, "No type available for" + buckettype);
        }
        aggregations.forEach((name, def) -> builder.subAggregation(def.asAggregationBuilder(name)));
        return builder;
    }

    public static class SubAggregation
    {
        private String field;
        private String aggtype;

        @JsonCreator
        public SubAggregation(@JsonProperty String field, @JsonProperty String aggtype)
        {
            this.field = requireNonNull(field);
            this.aggtype = requireNonNull(aggtype);
        }

        @JsonProperty("field")
        public String getField()
        {
            return field;
        }

        @JsonProperty("aggtype")
        public String getAggtype()
        {
            return aggtype;
        }

        AggregationBuilder asAggregationBuilder(String name)
        {
            switch (aggtype) {
                case "min":
                    return new MinAggregationBuilder(name)
                        .field(field);
                case "max":
                    return new MaxAggregationBuilder(name)
                        .field(field);
                case "avg":
                    return new AvgAggregationBuilder(name)
                        .field(field);
                case "sum":
                    return new SumAggregationBuilder(name)
                        .field(field);
            }
            throw new PrestoException(NOT_SUPPORTED, "No ES aggregation " + aggtype);
        }
    }
/*
    public static void main(String[] args)
    {
        ObjectMapper objectMapper = new ObjectMapperProvider().get().enable(MapperFeature.CAN_OVERRIDE_ACCESS_MODIFIERS);
        try {
            ElasticsearchAggregation test = objectMapper.readValue(
                "{\"bucketfield\": \"bar\", \"buckettype\": \"terms\", \"extrabucketattributes\": {\"interval\": 30000}, " +
                    "\"aggregations\":{\"disk_space_fee_min\": {\"aggtype\": \"min\", \"field\": \"disk_space_free\"}}} ", ElasticsearchAggregation.class);
            AggregationBuilder builder = test.buildAggregation();
            builder.getMetaData();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    */
}
