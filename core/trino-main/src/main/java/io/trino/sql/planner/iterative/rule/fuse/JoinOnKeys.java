package io.trino.sql.planner.iterative.rule.fuse;

import io.trino.Session;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.PlanNode;

import java.util.Optional;

import static io.trino.SystemSessionProperties.isFuseSubPlanEnabled;
import static io.trino.sql.planner.plan.Patterns.join;
import static io.trino.sql.tree.BooleanLiteral.TRUE_LITERAL;
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

        if (!(right instanceof AggregationNode)) {
            return Result.empty();
        }

        if (left instanceof JoinNode leftJoinNOde && leftJoinNOde.isCrossJoin()) {
            // in case of left leaning, n-ary join try to fold the left side first
            Result leftResult = apply(leftJoinNOde, null, context);
            if (leftResult.isEmpty()) {
                return Result.empty();
            }
            left = leftResult.getTransformedPlan().get();
        }

        if (!(left instanceof AggregationNode)) {
            // TODO lysy: this can be Filter(Aggregation) or similar and we can probably handle that
            return Result.empty();
        }

        Optional<PlanNodeFuser.FusedPlanNode> maybeFused = new PlanNodeFuser(context).fuse(left, right);
        Result result = maybeFused
                .map(fused -> {
                    if (fused.leftFilter().equals(TRUE_LITERAL) && fused.rightFilter().equals(TRUE_LITERAL)) {
                        return Result.ofPlanNode(fused.plan());
                    }

                    return Result.ofPlanNode(new FilterNode(
                            context.getIdAllocator().getNextId(),
                            fused.plan(),
                            and(fused.leftFilter(), fused.rightFilter())));
                })
                .orElse(Result.empty());
        return result;
    }
}
