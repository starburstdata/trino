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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import io.trino.execution.Lifespan;
import io.trino.operator.cache.PlanSignatureNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanNodeId;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class DriverFactory
{
    private final Optional<PlanSignatureNode> planSignature;
    private final int pipelineId;
    private final boolean inputDriver;
    private final boolean outputDriver;
    private final List<OperatorFactory> operatorFactories;
    private final Optional<PlanNodeId> sourceId;
    private final OptionalInt driverInstances;
    private final PipelineExecutionStrategy pipelineExecutionStrategy;

    private boolean closed;
    private final Set<Lifespan> encounteredLifespans = new HashSet<>();
    private final Set<Lifespan> closedLifespans = new HashSet<>();

    public DriverFactory(int pipelineId, boolean inputDriver, boolean outputDriver, List<OperatorFactory> operatorFactories, OptionalInt driverInstances, PipelineExecutionStrategy pipelineExecutionStrategy)
    {
        this(Optional.empty(), pipelineId, inputDriver, outputDriver, operatorFactories, driverInstances, pipelineExecutionStrategy);
    }

    public DriverFactory(PlanNode plan, int pipelineId, boolean inputDriver, boolean outputDriver, List<OperatorFactory> operatorFactories, OptionalInt driverInstances, PipelineExecutionStrategy pipelineExecutionStrategy)
    {
        this(PlanSignatureNode.from(plan), pipelineId, inputDriver, outputDriver, operatorFactories, driverInstances, pipelineExecutionStrategy);
    }

    public DriverFactory(Optional<PlanSignatureNode> planSignature, int pipelineId, boolean inputDriver, boolean outputDriver, List<OperatorFactory> operatorFactories, OptionalInt driverInstances, PipelineExecutionStrategy pipelineExecutionStrategy)
    {
        this.planSignature = planSignature;
        this.pipelineId = pipelineId;
        this.inputDriver = inputDriver;
        this.outputDriver = outputDriver;
        this.operatorFactories = ImmutableList.copyOf(requireNonNull(operatorFactories, "operatorFactories is null"));
        checkArgument(!operatorFactories.isEmpty(), "There must be at least one operator");
        this.driverInstances = requireNonNull(driverInstances, "driverInstances is null");
        this.pipelineExecutionStrategy = requireNonNull(pipelineExecutionStrategy, "pipelineExecutionStrategy is null");

        List<PlanNodeId> sourceIds = operatorFactories.stream()
                .filter(SourceOperatorFactory.class::isInstance)
                .map(SourceOperatorFactory.class::cast)
                .map(SourceOperatorFactory::getSourceId)
                .collect(toImmutableList());
        checkArgument(sourceIds.size() <= 1, "Expected at most one source operator in driver factory, but found %s", sourceIds);
        this.sourceId = sourceIds.isEmpty() ? Optional.empty() : Optional.of(sourceIds.get(0));
    }

    public int getPipelineId()
    {
        return pipelineId;
    }

    public boolean isInputDriver()
    {
        return inputDriver;
    }

    public boolean isOutputDriver()
    {
        return outputDriver;
    }

    /**
     * return the sourceId of this DriverFactory.
     * A DriverFactory doesn't always have source node.
     * For example, ValuesNode is not a source node.
     */
    public Optional<PlanNodeId> getSourceId()
    {
        return sourceId;
    }

    public OptionalInt getDriverInstances()
    {
        return driverInstances;
    }

    public PipelineExecutionStrategy getPipelineExecutionStrategy()
    {
        return pipelineExecutionStrategy;
    }

    public List<OperatorFactory> getOperatorFactories()
    {
        return operatorFactories;
    }

    public synchronized Driver createDriver(DriverContext driverContext)
    {
        checkState(!closed, "DriverFactory is already closed");
        requireNonNull(driverContext, "driverContext is null");
        checkState(!closedLifespans.contains(driverContext.getLifespan()), "DriverFactory is already closed for driver group %s", driverContext.getLifespan());
        encounteredLifespans.add(driverContext.getLifespan());
        ImmutableList.Builder<Operator> operators = ImmutableList.builder();
        for (OperatorFactory operatorFactory : operatorFactories) {
            Operator operator = operatorFactory.createOperator(driverContext);
            operators.add(operator);
        }
        return Driver.createDriver(driverContext, planSignature, operators.build());
    }

    public synchronized void noMoreDrivers(Lifespan lifespan)
    {
        if (closedLifespans.contains(lifespan)) {
            return;
        }
        encounteredLifespans.add(lifespan);
        closedLifespans.add(lifespan);
        for (OperatorFactory operatorFactory : operatorFactories) {
            operatorFactory.noMoreOperators(lifespan);
        }
    }

    public synchronized void noMoreDrivers()
    {
        if (closed) {
            return;
        }
        if (encounteredLifespans.size() != closedLifespans.size()) {
            Sets.difference(encounteredLifespans, closedLifespans).forEach(this::noMoreDrivers);
            verify(encounteredLifespans.size() == closedLifespans.size());
        }
        closed = true;
        for (OperatorFactory operatorFactory : operatorFactories) {
            operatorFactory.noMoreOperators();
        }
    }
}
