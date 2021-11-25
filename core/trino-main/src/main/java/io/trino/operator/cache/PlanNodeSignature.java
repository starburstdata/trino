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

import com.google.common.base.MoreObjects;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanVisitor;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.TableScanNode;

import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public abstract class PlanNodeSignature
{
    public static Optional<PlanNodeSignature> from(PlanNode plan)
    {
        Optional<PlanNodeSignature> signatureNode = plan.accept(new Visitor(), null);
        return signatureNode;
    }

    private static class Visitor
            extends PlanVisitor<Optional<PlanNodeSignature>, Void>
    {
        @Override
        protected Optional<PlanNodeSignature> visitPlan(PlanNode node, Void context)
        {
            System.out.println("missing signature for: " + node);
            return Optional.empty();
        }

        @Override
        public Optional<PlanNodeSignature> visitAggregation(AggregationNode node, Void context)
        {
            return node.getSource().accept(this, context).map(source -> new AggregationNodeSignature(source, node));
        }

        @Override
        public Optional<PlanNodeSignature> visitTableScan(TableScanNode node, Void context)
        {
            return Optional.of(new TableScanNodeSignature(node));
        }

        @Override
        public Optional<PlanNodeSignature> visitProject(ProjectNode node, Void context)
        {
            return node.getSource().accept(this, context).map(source -> new ProjectNodeSignature(source, node));
        }

        @Override
        public Optional<PlanNodeSignature> visitFilter(FilterNode node, Void context)
        {
            return node.getSource().accept(this, context).map(source -> new FilterNodeSignature(source, node));
        }
    }

    private static class TableScanNodeSignature
            extends PlanNodeSignature
    {
        private final TableScanNode node;

        private TableScanNodeSignature(TableScanNode node)
        {
            this.node = requireNonNull(node, "node is null");
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
            return MoreObjects.toStringHelper(this)
                    .add("node", node)
                    .toString();
        }
    }

    private static class AggregationNodeSignature
            extends PlanNodeSignature
    {
        private final PlanNodeSignature source;
        private final AggregationNode node;

        public AggregationNodeSignature(PlanNodeSignature source, AggregationNode node)
        {
            this.source = requireNonNull(source, "source is null");
            this.node = requireNonNull(node, "node is null");
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
                    node.getOutputSymbols()
            );
        }

        @Override
        public String toString()
        {
            return MoreObjects.toStringHelper(this)
                    .add("source", source)
                    .add("node", node)
                    .toString();
        }
    }

    private static class ProjectNodeSignature
            extends PlanNodeSignature
    {
        private final PlanNodeSignature source;
        private final ProjectNode node;

        public ProjectNodeSignature(PlanNodeSignature source, ProjectNode node)
        {
            this.source = requireNonNull(source, "source is null");
            this.node = requireNonNull(node, "node is null");
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
            return MoreObjects.toStringHelper(this)
                    .add("source", source)
                    .add("node", node)
                    .toString();
        }
    }

    private static class FilterNodeSignature
            extends PlanNodeSignature
    {
        private final PlanNodeSignature source;
        private final FilterNode node;

        public FilterNodeSignature(PlanNodeSignature source, FilterNode node)
        {
            this.source = requireNonNull(source, "source is null");
            this.node = requireNonNull(node, "node is null");
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
            return MoreObjects.toStringHelper(this)
                    .add("source", source)
                    .add("node", node)
                    .toString();
        }
    }
}
