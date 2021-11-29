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
package io.trino.operator.cache;

import io.airlift.log.Logger;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanVisitor;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.TableScanNode;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public interface PlanNodeSignature
{
    Logger log = Logger.get(CachingDriver.class);

    PlanNodeSignature UNKNOWN = () -> false;

    boolean canCache();

    static PlanNodeSignature from(PlanNode plan)
    {
        return plan.accept(new Visitor(), null);
    }

    class Visitor
            extends PlanVisitor<PlanNodeSignature, Void>
    {
        @Override
        protected PlanNodeSignature visitPlan(PlanNode node, Void context)
        {
            log.debug("missing signature for: " + node);
            return new GenericPlanNodeSignature(
                    false,
                    node,
                    node.getSources().stream()
                            .map(source -> source.accept(this, context))
                            .collect(toImmutableList()));
        }

        @Override
        public PlanNodeSignature visitAggregation(AggregationNode node, Void context)
        {
            return new AggregationNodeSignature(node.getSource().accept(this, context), node);
        }

        @Override
        public PlanNodeSignature visitTableScan(TableScanNode node, Void context)
        {
            return new TableScanNodeSignature(node);
        }

        @Override
        public PlanNodeSignature visitProject(ProjectNode node, Void context)
        {
            return new ProjectNodeSignature(node.getSource().accept(this, context), node);
        }

        @Override
        public PlanNodeSignature visitFilter(FilterNode node, Void context)
        {
            return new FilterNodeSignature(node.getSource().accept(this, context), node);
        }

        @Override
        public PlanNodeSignature visitJoin(JoinNode node, Void context)
        {
            return new JoinNodeSignature(
                    node.getLeft().accept(this, context),
                    node.getRight().accept(this, context),
                    node);
        }
    }

    class TableScanNodeSignature
            implements PlanNodeSignature
    {
        private final TableScanNode node;

        private TableScanNodeSignature(TableScanNode node)
        {
            this.node = requireNonNull(node, "node is null");
        }

        @Override
        public boolean canCache()
        {
            return true;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TableScanNodeSignature that = (TableScanNodeSignature) o;
            return node.isUpdateTarget() == that.node.isUpdateTarget()
                    && node.getTable().getConnectorHandle().equals(that.node.getTable().getConnectorHandle())
                    && node.getOutputSymbols().equals(that.node.getOutputSymbols())
                    && node.getAssignments().equals(that.node.getAssignments());
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(
                    node.getTable().getConnectorHandle(),
                    node.getOutputSymbols(),
                    node.getAssignments());
        }

        @Override
        public String toString()
        {
            return node.toString();
        }
    }

    class AggregationNodeSignature
            implements PlanNodeSignature
    {
        private final PlanNodeSignature source;
        private final AggregationNode node;

        public AggregationNodeSignature(PlanNodeSignature source, AggregationNode node)
        {
            this.source = requireNonNull(source, "source is null");
            this.node = requireNonNull(node, "node is null");
        }

        @Override
        public boolean canCache()
        {
            return source.canCache();
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            AggregationNodeSignature that = (AggregationNodeSignature) o;
            return source.equals(that.source)
                    && node.getAggregations().equals(that.node.getAggregations())
                    && node.getGroupingSets().equals(that.node.getGroupingSets())
                    && node.getPreGroupedSymbols().equals(that.node.getPreGroupedSymbols())
                    && node.getStep() == that.node.getStep()
                    && node.getHashSymbol().equals(that.node.getHashSymbol())
                    && node.getGroupIdSymbol().equals(that.node.getGroupIdSymbol())
                    && node.getOutputSymbols().equals(that.node.getOutputSymbols());
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(
                    source,
                    node.getAggregations(),
                    node.getGroupingSets(), node.getPreGroupedSymbols(),
                    node.getStep(),
                    node.getHashSymbol(),
                    node.getGroupIdSymbol(),
                    node.getOutputSymbols());
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("source", source)
                    .add("node", node)
                    .toString();
        }
    }

    class ProjectNodeSignature
            implements PlanNodeSignature
    {
        private final PlanNodeSignature source;
        private final ProjectNode node;

        public ProjectNodeSignature(PlanNodeSignature source, ProjectNode node)
        {
            this.source = requireNonNull(source, "source is null");
            this.node = requireNonNull(node, "node is null");
        }

        @Override
        public boolean canCache()
        {
            return source.canCache();
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ProjectNodeSignature that = (ProjectNodeSignature) o;
            return source.equals(that.source)
                    && node.getAssignments().equals(that.node.getAssignments());
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(
                    source,
                    node.getAssignments());
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("source", source)
                    .add("assignments", node.getAssignments())
                    .toString();
        }
    }

    class FilterNodeSignature
            implements PlanNodeSignature
    {
        private final PlanNodeSignature source;
        private final FilterNode node;

        public FilterNodeSignature(PlanNodeSignature source, FilterNode node)
        {
            this.source = requireNonNull(source, "source is null");
            this.node = requireNonNull(node, "node is null");
        }

        @Override
        public boolean canCache()
        {
            return source.canCache();
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            FilterNodeSignature that = (FilterNodeSignature) o;
            return source.equals(that.source)
                    && node.getPredicate().equals(that.node.getPredicate());
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(
                    source,
                    node.getPredicate());
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("source", source)
                    .add("predicate", node.getPredicate())
                    .toString();
        }
    }

    class JoinNodeSignature
            implements PlanNodeSignature
    {
        private final PlanNodeSignature left;
        private final PlanNodeSignature right;
        private final JoinNode node;

        public JoinNodeSignature(PlanNodeSignature left, PlanNodeSignature right, JoinNode node)
        {
            this.left = requireNonNull(left, "source is null");
            this.right = requireNonNull(right, "right is null");
            this.node = requireNonNull(node, "node is null");
        }

        @Override
        public boolean canCache()
        {
            return left.canCache();
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            JoinNodeSignature that = (JoinNodeSignature) o;
            return left.equals(that.left)
                    && right.equals(that.right)
                    && node.isMaySkipOutputDuplicates() == that.node.isMaySkipOutputDuplicates()
                    && node.getType() == that.node.getType()
                    && node.getCriteria().equals(that.node.getCriteria())
                    && node.getLeftOutputSymbols().equals(that.node.getLeftOutputSymbols())
                    && node.getRightOutputSymbols().equals(that.node.getRightOutputSymbols())
                    && node.getFilter().equals(that.node.getFilter())
                    && node.getLeftHashSymbol().equals(that.node.getLeftHashSymbol())
                    && node.getRightHashSymbol().equals(that.node.getRightHashSymbol())
                    && node.getDistributionType().equals(that.node.getDistributionType())
                    && node.isSpillable().equals(that.node.isSpillable())
                    && node.getDynamicFilters().equals(that.node.getDynamicFilters());
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(
                    left,
                    right,
                    node.getType(),
                    node.getCriteria(),
                    node.getLeftOutputSymbols(),
                    node.getRightOutputSymbols(),
                    node.isMaySkipOutputDuplicates(),
                    node.getFilter(),
                    node.getLeftHashSymbol(),
                    node.getRightHashSymbol(),
                    node.getDistributionType(),
                    node.isSpillable(),
                    node.getDynamicFilters());
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("type", node.getType())
                    .add("left", left)
                    .add("right", right)
                    .add("criteria", node.getCriteria())
                    .add("filter", node.getFilter())
                    .add("leftHashSymbol", node.getLeftHashSymbol())
                    .add("rightHashSymbol", node.getRightHashSymbol())
                    .add("distributionType", node.getDistributionType())
                    .toString();
        }
    }

    class GenericPlanNodeSignature
            implements PlanNodeSignature
    {
        private final boolean canCache;
        private final PlanNode node;
        private final List<PlanNodeSignature> sources;

        public GenericPlanNodeSignature(boolean canCache, PlanNode node, List<PlanNodeSignature> sources)
        {
            this.canCache = canCache;
            this.node = requireNonNull(node, "node is null");
            this.sources = requireNonNull(sources, "sources is null");
        }

        public boolean canCache()
        {
            return canCache;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            GenericPlanNodeSignature that = (GenericPlanNodeSignature) o;
            return node.getClass().equals(that.node.getClass()) && sources.equals(that.sources);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(node.getClass(), sources);
        }
    }
}
