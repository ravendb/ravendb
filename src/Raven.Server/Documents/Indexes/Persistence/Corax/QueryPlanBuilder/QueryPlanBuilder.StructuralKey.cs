using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using Corax.Querying.Planning;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.AST;
using Sparrow;
using IndexSearcher = Corax.Querying.IndexSearcher;

namespace Raven.Server.Documents.Indexes.Persistence.Corax.QueryPlanBuilder;

internal static partial class QueryPlanBuilder
{
    // Node tags for the structural-key bit stream. Every recursive operand position starts with one of these, so
    // the packed stream is self-delimiting and two structurally distinct trees can never collide.
    private enum AstTag
    {
        Null = 0,
        Binary = 1,
        Negated = 2,
        Between = 3,
        In = 4,
        Method = 5,
        Field = 6,
        Value = 7,
        True = 8,
        QuotedField = 9, // a reserved-word field name quoted in the source ('Order'), parsed as a string value
    }

    // Width, in bits, of an AstTag in the packed stream. Holds every member above with room to spare.
    private const int AstTagBits = 4;

    // Width, in bits, of an OperatorType (8 members); 4 bits leaves headroom.
    private const int OperatorBits = 4;

    // Width, in bits, of a ValueTokenType (7 members) - used for literal-type tokens and ORDER BY argument types.
    private const int ValueTokenBits = 4;

    // Width, in bits, of an OrderByFieldType (9 members).
    private const int OrderingTypeBits = 4;

    // Width, in bits, of a NullsOrderingType (3 members).
    private const int NullsOrderingBits = 2;

    // Width, in bits, of a MethodType. The enum churns (currently ~47 members), so keep a wide field (capacity
    // 256): a release-mode overflow would silently truncate the value and collide otherwise-distinct plan keys.
    private const int MethodTypeBits = 8;

    private static void AppendTag(ref PlanCacheKeyBuilder builder, AstTag tag) => builder.Append((int)tag, AstTagBits);

    internal static void ValidateStructuralKeyBitWidths()
    {
        AssertEnumFitsInBits<AstTag>(AstTagBits, nameof(AstTagBits));
        AssertEnumFitsInBits<OperatorType>(OperatorBits, nameof(OperatorBits));
        AssertEnumFitsInBits<ValueTokenType>(ValueTokenBits, nameof(ValueTokenBits));
        AssertEnumFitsInBits<OrderByFieldType>(OrderingTypeBits, nameof(OrderingTypeBits));
        AssertEnumFitsInBits<NullsOrderingType>(NullsOrderingBits, nameof(NullsOrderingBits));
        AssertEnumFitsInBits<MethodType>(MethodTypeBits, nameof(MethodTypeBits));

        static void AssertEnumFitsInBits<TEnum>(int bits, string constantName) where TEnum : struct, Enum
        {
            long capacity = 1L << bits;
            foreach (var member in Enum.GetValues<TEnum>())
            {
                long value = Convert.ToInt64(member);
                if (value < 0 || value >= capacity)
                    throw new InvalidOperationException(
                        $"Structural-key bit field {constantName} = {bits} bits (capacity {capacity}) cannot encode " +
                        $"{typeof(TEnum).Name}.{member} = {value}. Widen {constantName} and confirm the packed stream still fits.");
            }
        }
    }

  
    // Structural plan key: a SHA256 digest over a canonical structural WHERE +ORDER BY AST.
    // Note that other things (SELECT / LIMIT, etc) do NOT affect the cache key, since they don't impact the plan
    [SkipLocalsInit]
    private static Vector256<long> ComputeStructuralKey(PlanParameters planParams)
    {
        var builder = new PlanCacheKeyBuilder();
        
        AppendCanonicalExpression(ref builder, planParams.Metadata.Query.Where, planParams.IndexSearcher);
        AppendCanonicalOrderBy(ref builder, planParams.Metadata.OrderBy, planParams.IndexSearcher);

        return builder.ToHash();
    }

    // Append a field name plus the field's single/multi-valued bit (since that impacts the query plan)
    private static void AppendFieldName(ref PlanCacheKeyBuilder builder, string name, IndexSearcher searcher)
    {
        AppendString(ref builder, name);
        builder.Append(name != null && searcher.HasMultipleTermsInField(name) ? 1 : 0, 1);
    }

    // Append a string as a presence bit, a length prefix, then its UTF-16 bytes copied directly into the buffer.
    // Allocation-free and exact: null, empty, and any two distinct strings each produce a different bit sequence.
    private static void AppendString(ref PlanCacheKeyBuilder builder, string value)
    {
        if (value == null)
        {
            builder.Append(0, 1);
            return;
        }

        builder.Append(1, 1);
        builder.Append(value.Length, 31);
        builder.Append(MemoryMarshal.AsBytes(value.AsSpan()));
    }

    private static void AppendCanonicalExpression(ref PlanCacheKeyBuilder builder, QueryExpression expr, IndexSearcher searcher, bool exactValues = false)
    {
        RuntimeHelpers.EnsureSufficientExecutionStack();
        switch (expr)
        {
            case null:
                AppendTag(ref builder, AstTag.Null); // no WHERE clause - distinct from any concrete node
                return;

            case BinaryExpression be:
                AppendTag(ref builder, AstTag.Binary);
                builder.Append((int)be.Operator, OperatorBits);
                if (be.Operator is OperatorType.And or OperatorType.Or)
                {
                    AppendCanonicalExpression(ref builder, be.Left, searcher, exactValues);
                    AppendCanonicalExpression(ref builder, be.Right, searcher, exactValues);
                }
                else
                {
                    AppendCanonicalField(ref builder, be.Left, searcher, exactValues);
                    AppendCanonicalExpression(ref builder, be.Right, searcher, exactValues);
                }

                return;

            case NegatedExpression ne:
                AppendTag(ref builder, AstTag.Negated);
                AppendCanonicalExpression(ref builder, ne.Expression, searcher, exactValues);
                return;

            case BetweenExpression bw:
                AppendTag(ref builder, AstTag.Between);
                AppendCanonicalField(ref builder, bw.Source, searcher, exactValues);
                AppendCanonicalValue(ref builder, bw.Min.Value, bw.Min.Token.Value, exactValues);
                AppendCanonicalValue(ref builder, bw.Max.Value, bw.Max.Token.Value, exactValues);
                builder.Append(bw.MinInclusive.ToInt32(), 1);
                builder.Append(bw.MaxInclusive.ToInt32(), 1);
                return;

            case InExpression ie:
                AppendTag(ref builder, AstTag.In);
                builder.Append(ie.All.ToInt32(), 1);
                AppendCanonicalField(ref builder, ie.Source, searcher, exactValues);
                // IN arity is structural: each value is its own binding, so (a,b) and (a,b,c) are different templates.
                builder.Append(ie.Values.Count, 31);
                foreach (var v in ie.Values)
                    AppendCanonicalExpression(ref builder, v, searcher, exactValues);
                return;

            case MethodExpression me:
            {
                AppendTag(ref builder, AstTag.Method);
                AppendString(ref builder, me.Name.Value);
                builder.Append(me.Arguments.Count, 31);

                // with auto-parameterization for literals, but for when, when($p == 1.5, X) and when($p == 2.0, X)
                // we disable that for the first argument for when() clauses
                bool isWhen = QueryMethod.GetMethodType(me.Name.Value, throwIfNoMatch: false) == MethodType.When;

                int firstOperand = 0;
                if (me.Arguments.Count > 0 && MethodTakesFieldAsFirstArgument(me.Name.Value))
                { // search() / regex() - etc - takes a _field_ name as their first arg, reflect that here.
                    AppendCanonicalField(ref builder, me.Arguments[0], searcher, exactValues);
                    firstOperand = 1;
                }

                for (int i = firstOperand; i < me.Arguments.Count; i++)
                {
                    AppendCanonicalExpression(ref builder, me.Arguments[i], searcher, exactValues || (isWhen && i == 0));
                }

                return;
            }

            case FieldExpression fe:
                AppendTag(ref builder, AstTag.Field);
                AppendFieldName(ref builder, fe.FieldValue, searcher);
                return;

            case ValueExpression ve:
                AppendTag(ref builder, AstTag.Value);
                AppendCanonicalValue(ref builder, ve.Value, ve.Token.Value, exactValues);
                return;

            case TrueExpression:
                AppendTag(ref builder, AstTag.True);
                return;

            default:
                throw new InvalidOperationException(
                    $"Unsupported query expression node type '{expr.GetType().Name}' in the structural plan key.");
        }
    }

    private static void AppendCanonicalValue(ref PlanCacheKeyBuilder builder, ValueTokenType valueTokenType, string value, bool exactValues = false)
    {
        if (valueTokenType == ValueTokenType.Parameter)
        {
            builder.Append(0, 1); // 0 = parameter operand
            return;
        }

        builder.Append(1, 1); // 1 = literal operand
        builder.Append((int)valueTokenType, ValueTokenBits);
        // Inside a constant-folded when() condition the literal VALUE effects the query plan, so it must be part of the key. 
        if (exactValues)
            AppendString(ref builder, value);
    }

    private static void AppendCanonicalField(ref PlanCacheKeyBuilder builder, QueryExpression expr, IndexSearcher searcher, bool exactValues = false)
    {
        switch (expr)
        {
            case FieldExpression fe:
                AppendTag(ref builder, AstTag.Field);
                AppendFieldName(ref builder, fe.FieldValue, searcher);
                return;

            case ValueExpression { Value: ValueTokenType.String } ve:
                AppendTag(ref builder, AstTag.QuotedField);
                AppendFieldName(ref builder, ve.Token.Value, searcher);
                return;

            default:
                AppendCanonicalExpression(ref builder, expr, searcher, exactValues);
                return;
        }
    }

    private static bool MethodTakesFieldAsFirstArgument(string methodName) =>
        QueryMethod.GetMethodType(methodName, throwIfNoMatch: false) switch
        {
            MethodType.Search
                or MethodType.StartsWith
                or MethodType.EndsWith
                or MethodType.Regex
                or MethodType.Exists
                or MethodType.Spatial_Within
                or MethodType.Spatial_Contains
                or MethodType.Spatial_Disjoint
                or MethodType.Spatial_Intersects
                or MethodType.Vector_Search => true,
            _ => false
        };

    private static void AppendCanonicalOrderBy(ref PlanCacheKeyBuilder builder, OrderByField[] orderBy, IndexSearcher searcher)
    {
        if (orderBy == null)
        {
            builder.Append(0, 1); // a null ORDER BY is kept distinct from an empty one
            return;
        }

        builder.Append(1, 1);
        builder.Append(orderBy.Length, 31);
        foreach (var field in orderBy)
        {
            AppendFieldName(ref builder, field.Name?.Value, searcher);
            builder.Append((int)field.OrderingType, OrderingTypeBits);
            builder.Append(field.Ascending.ToInt32(), 1);
            builder.Append((int)field.NullsOrdering, NullsOrderingBits);
            if (field.Method.HasValue)
            {
                builder.Append(1, 1);
                builder.Append((int)field.Method.Value, MethodTypeBits);
            }
            else
            {
                builder.Append(0, 1);
            }

            if (field.Arguments == null)
            {
                builder.Append(0, 1);
            }
            else
            {
                builder.Append(1, 1);
                builder.Append(field.Arguments.Length, 31);
                foreach (var arg in field.Arguments)
                {
                    AppendCanonicalValue(ref builder, arg.Type, arg.NameOrValue);
                }
            }
        }
    }
}
