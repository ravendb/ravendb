using System.Collections.Generic;
using System.Globalization;
using System.Runtime.CompilerServices;
using Raven.Server.Documents.Queries.AST;

namespace Raven.Server.Documents.Queries;

/// <summary>
/// Rewrites a where clause once, when its query metadata is built, so every request sharing the cached query text gets
/// the cheaper shape: a lower and an upper bound on the same field become one <see cref="BetweenExpression"/> regardless
/// of how the 'and' chain is nested or parenthesized, duplicated bounds are dropped, literal bounds in the same direction
/// keep only the tighter one, and same-direction bounds whose values are parameters are moved next to each other so the
/// query builders can keep the tighter one once the values are known. Only the structure is looked at, never a parameter
/// value, because the cached metadata serves requests with different parameter values. The input tree is never mutated
/// and unchanged subtrees come back as the same instances.
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

            default:
                normalized = expression;
                return false;
        }
    }

    private static bool TryNormalizeAndChain(BinaryExpression root, out QueryExpression normalized)
    {
        var operands = new List<QueryExpression>();
        var changed = Flatten(root, operands);

        changed |= DropDuplicateBounds(operands);
        changed |= MergeLiteralBoundsInTheSameDirection(operands);
        changed |= FoldOppositeBoundsIntoBetween(operands);
        changed |= GroupBoundsInTheSameDirection(operands);

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

    // 'Foo > 1 and Foo > 2' is 'Foo > 2'; only literals can be compared here, parameters are left to the query builders
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
                    MaxInclusive = less.Operator == OperatorType.LessThanEqual
                };
                operands.RemoveAt(j);
                changed = true;
                break;
            }
        }

        return changed;
    }

    // same-direction bounds with parameter values are placed next to each other, so the query builders, which know the
    // values, find them as adjacent siblings and keep the tighter one
    private static bool GroupBoundsInTheSameDirection(List<QueryExpression> operands)
    {
        var changed = false;
        for (var i = 0; i < operands.Count; i++)
        {
            if (IsBound(operands[i], out var first) == false)
                continue;

            var insertAt = i + 1;
            for (var j = i + 1; j < operands.Count; j++)
            {
                if (IsBound(operands[j], out var other) == false || OnSameField(first, other) == false || first.IsGreaterThan != other.IsGreaterThan)
                    continue;

                if (j != insertAt)
                {
                    operands.RemoveAt(j);
                    operands.Insert(insertAt, other);
                    changed = true;
                }

                insertAt++;
            }
        }

        return changed;
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

    private static bool TryCompareNumericLiterals(ValueExpression first, ValueExpression second, out int comparison)
    {
        comparison = 0;

        if (first.Value == ValueTokenType.Long && second.Value == ValueTokenType.Long)
        {
            comparison = QueryBuilderHelper.ParseInt64WithSeparators(first.Token.Value).CompareTo(QueryBuilderHelper.ParseInt64WithSeparators(second.Token.Value));
            return true;
        }

        if (TryGetNumericLiteral(first, out var firstNumber) == false || TryGetNumericLiteral(second, out var secondNumber) == false)
            return false;

        comparison = firstNumber.CompareTo(secondNumber);
        return true;
    }

    private static bool TryGetNumericLiteral(ValueExpression value, out double number)
    {
        switch (value.Value)
        {
            case ValueTokenType.Long:
                number = QueryBuilderHelper.ParseInt64WithSeparators(value.Token.Value);
                return true;
            case ValueTokenType.Double:
                return double.TryParse(value.Token.Value, NumberStyles.Float, CultureInfo.InvariantCulture, out number);
            default:
                number = 0;
                return false;
        }
    }
}
