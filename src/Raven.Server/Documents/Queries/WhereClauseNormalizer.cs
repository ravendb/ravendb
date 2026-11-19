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
    public static QueryExpression Normalize(QueryExpression expression, string alias = null, bool pairsOnly = false)
    {
        return TryNormalize(expression, new Mode(alias, pairsOnly), out var normalized) ? normalized : expression;
    }

    public static bool TryNormalize(QueryExpression expression, out QueryExpression normalized)
    {
        return TryNormalize(expression, default, out normalized);
    }

    private readonly struct Mode
    {
        public readonly string Alias;
        public readonly bool PairsOnly;

        public Mode(string alias, bool pairsOnly)
        {
            Alias = alias;
            PairsOnly = pairsOnly;
        }
    }

    private static bool TryNormalize(QueryExpression expression, Mode mode, out QueryExpression normalized)
    {
        RuntimeHelpers.EnsureSufficientExecutionStack();

        switch (expression)
        {
            case BinaryExpression { Operator: OperatorType.And } and:
                return mode.PairsOnly ? TryFoldPairsInPlace(and, mode, out normalized) : TryNormalizeAndChain(and, mode, out normalized);

            case BinaryExpression { Operator: OperatorType.Or } or:
            {
                var leftChanged = TryNormalize(or.Left, mode, out var left);
                var rightChanged = TryNormalize(or.Right, mode, out var right);
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
                if (TryNormalize(not.Expression, mode, out var inner) == false)
                {
                    normalized = not;
                    return false;
                }

                normalized = new NegatedExpression(inner);
                return true;
            }

            case MethodExpression method:
                return TryNormalizeArguments(method, mode, out normalized);

            default:
                normalized = expression;
                return false;
        }
    }

    private static bool TryNormalizeArguments(MethodExpression method, Mode mode, out QueryExpression normalized)
    {
        var (first, count) = QueryMethod.GetMethodType(method.Name.Value, throwIfNoMatch: false) switch
        {
            // exact() and boost() wrap a where clause of their own
            MethodType.Exact or MethodType.Boost => (0, 1),
            MethodType.When => (1, 1),
            MethodType.Intersect => (0, method.Arguments.Count),
            _ => (0, 0)
        };

        List<QueryExpression> arguments = null;
        for (var i = first; i < first + count && i < method.Arguments.Count; i++)
        {
            if (TryNormalize(method.Arguments[i], mode, out var argument) == false)
                continue;

            arguments ??= new List<QueryExpression>(method.Arguments);
            arguments[i] = argument;
        }

        if (arguments == null)
        {
            normalized = method;
            return false;
        }

        normalized = new MethodExpression(method.Name, arguments) { Alias = method.Alias };
        return true;
    }

    private static bool TryFoldPairsInPlace(BinaryExpression root, Mode mode, out QueryExpression normalized)
    {
        var spine = new Stack<BinaryExpression>();
        QueryExpression current = root;
        while (current is BinaryExpression { Operator: OperatorType.And } and && IsPair(and, mode.Alias, out _, out _) == false)
        {
            spine.Push(and);
            current = and.Left;
        }

        bool changed;
        if (current is BinaryExpression { Operator: OperatorType.And } pair && IsPair(pair, mode.Alias, out var lower, out var upper))
        {
            normalized = Fold(lower, upper, siblings: true);
            changed = true;
        }
        else
        {
            changed = TryNormalize(current, mode, out normalized);
        }

        while (spine.Count > 0)
        {
            var node = spine.Pop();
            var rightChanged = TryNormalize(node.Right, mode, out var right);
            if (changed == false && rightChanged == false)
            {
                normalized = node;
                continue;
            }

            normalized = new BinaryExpression(normalized, right, OperatorType.And) { Parenthesis = node.Parenthesis };
            changed = true;
        }

        return changed;
    }

    private static bool TryNormalizeAndChain(BinaryExpression root, Mode mode, out QueryExpression normalized)
    {
        var operands = new List<QueryExpression>();
        var changed = Flatten(root, operands, mode);

        changed |= DropDuplicateBounds(operands, mode.Alias);
        changed |= MergeLiteralBoundsInTheSameDirection(operands, mode.Alias);
        changed |= FoldOppositeBoundsIntoBetween(operands, mode.Alias);
        changed |= GroupBoundsOnTheSameField(operands, mode.Alias);

        if (changed == false)
        {
            normalized = root;
            return false;
        }

        normalized = operands[0];
        for (var i = 1; i < operands.Count; i++)
            normalized = new BinaryExpression(normalized, operands[i], OperatorType.And);

        if (operands.Count > 1)
            ((BinaryExpression)normalized).Parenthesis = root.Parenthesis;

        return true;
    }

    // iterative on purpose: a chain of thousands of clauses must not cost one stack frame per clause
    private static bool Flatten(BinaryExpression root, List<QueryExpression> operands, Mode mode)
    {
        var changed = false;
        var pending = new Stack<QueryExpression>();
        pending.Push(root);

        while (pending.Count > 0)
        {
            var current = pending.Pop();
            if (current is BinaryExpression { Operator: OperatorType.And } and)
            {
                if (IsPair(and, mode.Alias, out var lower, out var upper))
                {
                    operands.Add(Fold(lower, upper, siblings: true));
                    changed = true;
                    continue;
                }

                pending.Push(and.Right);
                pending.Push(and.Left);
                continue;
            }

            changed |= TryNormalize(current, mode, out var operand);
            operands.Add(operand);
        }

        return changed;
    }

    // 'Foo > $a and Foo > $a' is the same bound twice, whatever $a turns out to be
    private static bool DropDuplicateBounds(List<QueryExpression> operands, string alias)
    {
        var changed = false;
        for (var i = 0; i < operands.Count; i++)
        {
            if (IsBound(operands[i], out var first) == false)
                continue;

            for (var j = operands.Count - 1; j > i; j--)
            {
                if (IsBound(operands[j], out var second) && first.Operator == second.Operator && IsSameField(first.Left, second.Left, alias) && first.Right.Equals(second.Right))
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
    private static bool MergeLiteralBoundsInTheSameDirection(List<QueryExpression> operands, string alias)
    {
        var changed = false;
        for (var i = 0; i < operands.Count; i++)
        {
            if (IsBound(operands[i], out var first) == false)
                continue;

            for (var j = i + 1; j < operands.Count; j++)
            {
                if (IsBound(operands[j], out var second) == false || IsSameField(first.Left, second.Left, alias) == false || first.IsGreaterThan != second.IsGreaterThan)
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
    private static bool FoldOppositeBoundsIntoBetween(List<QueryExpression> operands, string alias)
    {
        var changed = false;
        for (var i = 0; i < operands.Count; i++)
        {
            if (IsBound(operands[i], out var first) == false)
                continue;

            for (var j = i + 1; j < operands.Count; j++)
            {
                if (IsBound(operands[j], out var second) == false || IsSameField(first.Left, second.Left, alias) == false || first.IsGreaterThan == second.IsGreaterThan)
                    continue;

                operands[i] = Fold(first, second, siblings: false);
                operands.RemoveAt(j);
                changed = true;
                break;
            }
        }

        return changed;
    }

    private static BetweenExpression Fold(BinaryExpression first, BinaryExpression second, bool siblings)
    {
        var (greater, less) = first.IsGreaterThan ? (first, second) : (second, first);

        return new BetweenExpression(first.Left, (ValueExpression)greater.Right, (ValueExpression)less.Right)
        {
            MinInclusive = greater.Operator == OperatorType.GreaterThanEqual,
            MaxInclusive = less.Operator == OperatorType.LessThanEqual,
            Folded = true,
            FoldedFromSiblings = siblings
        };
    }

    // a bound gathers the same-direction bounds on its field behind it, a folded between the bounds of either direction and
    // the other betweens, so the builders find what they must compare as adjacent siblings
    private static bool GroupBoundsOnTheSameField(List<QueryExpression> operands, string alias)
    {
        var changed = false;
        for (var i = 0; i < operands.Count; i++)
        {
            if (TryGetAnchor(operands[i], out var field, out var direction) == false)
                continue;

            var insertAt = i + 1;
            for (var j = i + 1; j < operands.Count; j++)
            {
                if (Gathers(field, direction, operands[j], alias) == false)
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

    private static bool Gathers(QueryExpression field, bool? isGreaterThan, QueryExpression candidate, string alias)
    {
        switch (candidate)
        {
            case BinaryExpression bound when QueryBuilderHelper.IsRangeBound(bound):
                return IsSameField(field, bound.Left, alias) && (isGreaterThan == null || isGreaterThan == bound.IsGreaterThan);
            case BetweenExpression { Folded: true } between:
                return isGreaterThan == null && IsSameField(field, between.Source, alias);
            default:
                return false;
        }
    }

    private static bool IsPair(BinaryExpression and, string alias, out BinaryExpression left, out BinaryExpression right)
    {
        right = null;
        return IsBound(and.Left, out left) && IsBound(and.Right, out right) && left.IsGreaterThan != right.IsGreaterThan && IsSameField(left.Left, right.Left, alias);
    }

    private static bool IsBound(QueryExpression expression, out BinaryExpression bound)
    {
        bound = expression as BinaryExpression;
        return bound != null && QueryBuilderHelper.IsRangeBound(bound);
    }

    private static bool IsSameField(QueryExpression first, QueryExpression second, string alias)
    {
        if (first is FieldExpression firstField && second is FieldExpression secondField)
            return PathOf(firstField, alias) == PathOf(secondField, alias);

        return first.Equals(second);
    }

    private static string PathOf(FieldExpression field, string alias)
    {
        return alias != null && field.Compound.Count > 1 && field.Compound[0] == alias ? field.FieldValueWithoutAlias : field.FieldValue;
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
