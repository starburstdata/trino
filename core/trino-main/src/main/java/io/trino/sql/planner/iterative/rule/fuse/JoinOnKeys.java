package io.trino.sql.planner.iterative.rule.fuse;

import io.trino.Session;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.PlanNode;

import static io.trino.SystemSessionProperties.isFuseSubPlanEnabled;
import static io.trino.sql.planner.plan.Patterns.join;
import static io.trino.sql.tree.LogicalExpression.and;

public class JoinOnKeys
        implements Rule<JoinNode>
{
    private static final Pattern<JoinNode> PATTERN = join()
            .matching(JoinNode::isCrossJoin);

    @Override
    public Pattern<JoinNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isFuseSubPlanEnabled(session);
    }

    @Override
    public Result apply(JoinNode node, Captures captures, Context context)
    {
        PlanNode left = context.getLookup().resolve(node.getLeft());
        PlanNode right = context.getLookup().resolve(node.getRight());
        if (!(left instanceof AggregationNode && right instanceof AggregationNode)) {
            return Result.empty();
        }

        return new PlanNodeFuser(context).fuse(left, right)
                .map(fused -> Result.ofPlanNode(new FilterNode(
                        context.getIdAllocator().getNextId(),
                        fused.plan(),
                        and(fused.leftFilter(), fused.rightFilter()))))
                .orElse(Result.empty());
    }
}
