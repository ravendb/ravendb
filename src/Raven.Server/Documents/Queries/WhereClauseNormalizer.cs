using System.Collections.Generic;
using System.Globalization;
using System.Runtime.CompilerServices;
using Raven.Server.Documents.Queries.AST;

namespace Raven.Server.Documents.Queries;

/// <summary>
/// Rewrites a where clause once, when its query metadata is built: a lower and an upper bound on one field become a between
/// however the 'and' chain is nested. Looks at structure only, never at parameter values, and never mutates the input tree.
/// </summary>
public static class WhereClauseNormalizer
{
    public static QueryExpression Normalize(QueryExpression expression)
    {
        return TryNormalize(expression, out var normalized) ? normalized : expression;
    }

    public static bool TryNormalize(QueryExpression expression, out QueryExpression normalized)
    {
        RuntimeHelpers.EnsureSufficientExecutionStack();

        switch (expression)
        {
            case BinaryExpression { Operator: OperatorType.And } and:
                return TryNormalizeAndChain(and, out normalized);

            case BinaryExpression { Operator: OperatorType.Or } or:
            {
                var leftChanged = TryNormalize(or.Left, out var left);
                var rightChanged = TryNormalize(or.Right, out var right);
                if (leftChanged == false && rightChanged == false)
                {
                    normalized = or;
                    return false;
                }

                normalized = new BinaryExpression(left, right, OperatorType.Or) { Parenthesis = or.Parenthesis };
                return true;
            }

            case NegatedExpression not:
            {
                if (TryNormalize(not.Expression, out var inner) == false)
                {
                    normalized = not;
                    return false;
                }

                normalized = new NegatedExpression(inner);
                return true;
            }

            // exact() and boost() wrap a where clause of their own
            case MethodExpression { Arguments.Count: > 0 } method when WrapsAWhereClause(method):
            {
                if (TryNormalize(method.Arguments[0], out var inner) == false)
                {
                    normalized = method;
                    return false;
                }

                var arguments = new List<QueryExpression>(method.Arguments) { [0] = inner };
                normalized = new MethodExpression(method.Name, arguments) { Alias = method.Alias };
                return true;
            }

            default:
                normalized = expression;
                return false;
        }
    }

    private static bool WrapsAWhereClause(MethodExpression method)
    {
        return QueryMethod.GetMethodType(method.Name.Value, throwIfNoMatch: false) is MethodType.Exact or MethodType.Boost;
    }

    private static bool TryNormalizeAndChain(BinaryExpression root, out QueryExpression normalized)
    {
        var operands = new List<QueryExpression>();
        var changed = Flatten(root, operands);

        changed |= DropDuplicateBounds(operands);
        changed |= MergeLiteralBoundsInTheSameDirection(operands);
        changed |= FoldOppositeBoundsIntoBetween(operands);
        changed |= GroupBoundsOnTheSameField(operands);

        if (changed == false)
        {
            normalized = root;
            return false;
        }

        normalized = operands[0];
        for (var i = 1; i < operands.Count; i++)
            normalized = new BinaryExpression(normalized, operands[i], OperatorType.And);

        return true;
    }

    // iterative on purpose: a chain of thousands of clauses must not cost one stack frame per clause
    private static bool Flatten(BinaryExpression root, List<QueryExpression> operands)
    {
        var changed = false;
        var pending = new Stack<QueryExpression>();
        pending.Push(root);

        while (pending.Count > 0)
        {
            var current = pending.Pop();
            if (current is BinaryExpression { Operator: OperatorType.And } and)
            {
                pending.Push(and.Right);
                pending.Push(and.Left);
                continue;
            }

            changed |= TryNormalize(current, out var operand);
            operands.Add(operand);
        }

        return changed;
    }

    // 'Foo > $a and Foo > $a' is the same bound twice, whatever $a turns out to be
    private static bool DropDuplicateBounds(List<QueryExpression> operands)
    {
        var changed = false;
        for (var i = 0; i < operands.Count; i++)
        {
            if (IsBound(operands[i], out var first) == false)
                continue;

            for (var j = operands.Count - 1; j > i; j--)
            {
                if (IsBound(operands[j], out var second) && first.Operator == second.Operator && OnSameField(first, second) && first.Right.Equals(second.Right))
                {
                    operands.RemoveAt(j);
                    changed = true;
                }
            }
        }

        return changed;
    }

    // 'Foo > 1 and Foo > 2' is 'Foo > 2'; parameters and string literals are left to the builders, which know the values,
    // the analyzer and whether the field holds dates
    private static bool MergeLiteralBoundsInTheSameDirection(List<QueryExpression> operands)
    {
        var changed = false;
        for (var i = 0; i < operands.Count; i++)
        {
            if (IsBound(operands[i], out var first) == false)
                continue;

            for (var j = i + 1; j < operands.Count; j++)
            {
                if (IsBound(operands[j], out var second) == false || OnSameField(first, second) == false || first.IsGreaterThan != second.IsGreaterThan)
                    continue;

                if (TryCompareNumericLiterals((ValueExpression)first.Right, (ValueExpression)second.Right, out var comparison) == false)
                    continue;

                first = QueryBuilderHelper.TighterBound(first, second, comparison);
                operands[i] = first;
                operands.RemoveAt(j);
                j--;
                changed = true;
            }
        }

        return changed;
    }

    // 'Foo >= $a and ... and Foo < $b' is one bounded range scan instead of two open-ended ones
    private static bool FoldOppositeBoundsIntoBetween(List<QueryExpression> operands)
    {
        var changed = false;
        for (var i = 0; i < operands.Count; i++)
        {
            if (IsBound(operands[i], out var first) == false)
                continue;

            for (var j = i + 1; j < operands.Count; j++)
            {
                if (IsBound(operands[j], out var second) == false || OnSameField(first, second) == false || first.IsGreaterThan == second.IsGreaterThan)
                    continue;

                var (greater, less) = first.IsGreaterThan ? (first, second) : (second, first);

                operands[i] = new BetweenExpression(first.Left, (ValueExpression)greater.Right, (ValueExpression)less.Right)
                {
                    MinInclusive = greater.Operator == OperatorType.GreaterThanEqual,
                    MaxInclusive = less.Operator == OperatorType.LessThanEqual,
                    Folded = true
                };
                operands.RemoveAt(j);
                changed = true;
                break;
            }
        }

        return changed;
    }

    // a bound gathers the same-direction bounds on its field behind it, a folded between the bounds of either direction and
    // the other betweens, so the builders find what they must compare as adjacent siblings
    private static bool GroupBoundsOnTheSameField(List<QueryExpression> operands)
    {
        var changed = false;
        for (var i = 0; i < operands.Count; i++)
        {
            if (TryGetAnchor(operands[i], out var field, out var direction) == false)
                continue;

            var insertAt = i + 1;
            for (var j = i + 1; j < operands.Count; j++)
            {
                if (Gathers(field, direction, operands[j]) == false)
                    continue;

                if (j != insertAt)
                {
                    var other = operands[j];
                    operands.RemoveAt(j);
                    operands.Insert(insertAt, other);
                    changed = true;
                }

                insertAt++;
            }
        }

        return changed;
    }

    private static bool TryGetAnchor(QueryExpression expression, out QueryExpression field, out bool? isGreaterThan)
    {
        switch (expression)
        {
            case BinaryExpression bound when QueryBuilderHelper.IsRangeBound(bound):
                field = bound.Left;
                isGreaterThan = bound.IsGreaterThan;
                return true;
            case BetweenExpression { Folded: true } between:
                field = between.Source;
                isGreaterThan = null;
                return true;
            default:
                field = null;
                isGreaterThan = null;
                return false;
        }
    }

    private static bool Gathers(QueryExpression field, bool? isGreaterThan, QueryExpression candidate)
    {
        switch (candidate)
        {
            case BinaryExpression bound when QueryBuilderHelper.IsRangeBound(bound):
                return QueryBuilderHelper.IsSameField(field, bound.Left) && (isGreaterThan == null || isGreaterThan == bound.IsGreaterThan);
            case BetweenExpression { Folded: true } between:
                return isGreaterThan == null && QueryBuilderHelper.IsSameField(field, between.Source);
            default:
                return false;
        }
    }

    private static bool IsBound(QueryExpression expression, out BinaryExpression bound)
    {
        bound = expression as BinaryExpression;
        return bound != null && QueryBuilderHelper.IsRangeBound(bound);
    }

    private static bool OnSameField(BinaryExpression first, BinaryExpression second)
    {
        return QueryBuilderHelper.IsSameField(first.Left, second.Left);
    }

    // a long bound queries the long term of the field and a double bound the double term, so only bounds of one kind are
    // compared; two longs compare as longs, since past 2^53 a double cannot tell neighbours apart
    private static bool TryCompareNumericLiterals(ValueExpression first, ValueExpression second, out int comparison)
    {
        comparison = 0;

        if (first.Value != second.Value)
            return false;

        switch (first.Value)
        {
            case ValueTokenType.Long:
                comparison = QueryBuilderHelper.ParseInt64WithSeparators(first.Token.Value).CompareTo(QueryBuilderHelper.ParseInt64WithSeparators(second.Token.Value));
                return true;
            case ValueTokenType.Double:
                comparison = ParseDouble(first).CompareTo(ParseDouble(second));
                return true;
            default:
                return false;
        }

        static double ParseDouble(ValueExpression value) => double.Parse(value.Token.Value, NumberStyles.Float, CultureInfo.InvariantCulture);
    }
}
