using System;
using Corax.Mappings;
using Corax.Querying.Matches.Meta;

namespace Corax.Querying.Planning;

/// <summary>
/// Packed parameter reference — a 32-bit value encoding the type and index(es) of a clause's value:
/// (QueryExecution.LongValues / DoubleValues / StringValues).
/// 
///     bits [31:30] = value type (Long=0, Double=1, String=2, None=3)
///     bits [29:15] = first parameter index (0..32767)
///     bits [14:0]  = second parameter index (0..32767, 0x7FFF = no second param)
/// 
/// For simple predicates (Equals, GT, LT, etc.): Param1 = value index, Param2 = NoParam.
/// For BETWEEN: Param1 = low-bound index, Param2 = high-bound index (same-typed array).
/// For IN/AllIn: Param1 = start index into the typed array. Term count is stored separately
/// in ClauseInfo.InTermCount (not packed) because IN can exceed 32K terms.
/// For parameterless clauses (Exists): None sentinel.
/// </summary>
public readonly struct PackedParam
{
    public const int NoParamValue = 0x7FFF;
    private const int MaxIndex = 0x7FFF; // 32,767

    public const int TypeLong = 0;
    public const int TypeDouble = 1;
    public const int TypeString = 2;
    private const int TypeNone = 3;

    public static readonly PackedParam None = new((TypeNone << 30) | (NoParamValue << 15) | NoParamValue);

    private readonly int _value;

    private PackedParam(int raw) => _value = raw;

    public PackedParam(int type, int param1, int param2 = NoParamValue)
    {
        if (param1 > MaxIndex)
            ThrowLimitExceeded(param1);
        if (param2 != NoParamValue && param2 > MaxIndex)
            ThrowLimitExceeded(param2);
        _value = (type << 30) | ((param1 & 0x7FFF) << 15) | (param2 & 0x7FFF);
    }

    public int ValueType => (_value >>> 30) & 0x3;

    public int Param1 => (_value >>> 15) & 0x7FFF;

    public int Param2 => _value & 0x7FFF;

    public bool IsNone => _value == None._value;

    /// <summary>For IN/AllIn clauses: build a PackedParam pointing at the n-th IN term.</summary>
    public PackedParam WithTermOffset(int termIndex) => new(ValueType, Param1 + termIndex);

    public IQueryMatch TermQuery(FieldMetadata fieldMeta, IndexSearcher indexSearcher, QueryExecution exec)
    {
        return ValueType switch
        {
            TypeLong => indexSearcher.TermQuery(fieldMeta, exec.LongValues[Param1]),
            TypeDouble => indexSearcher.TermQuery(fieldMeta, exec.DoubleValues[Param1]),
            _ => indexSearcher.TermQuery(fieldMeta, exec.GetAnalyzedSlice(indexSearcher, fieldMeta, Param1))
        };
    }

    public long GetTermPostingListId(FieldMetadata fieldMeta, IndexSearcher indexSearcher, QueryExecution exec)
    {
        return ValueType switch
        {
            TypeLong => indexSearcher.GetTermPostingListId(fieldMeta, exec.LongValues[Param1]),
            TypeDouble => indexSearcher.GetTermPostingListId(fieldMeta, exec.DoubleValues[Param1]),
            _ => indexSearcher.GetTermPostingListId(fieldMeta, exec.StringValues[Param1])
        };
    }

    public IQueryMatch RangeQuery(ClauseType op, FieldMetadata fieldMeta, IndexSearcher indexSearcher, QueryExecution exec, bool forward = true)
    {
        return op switch
        {
            ClauseType.GreaterThan => ValueType switch
            {
                TypeLong => indexSearcher.GreaterThanQuery(fieldMeta, exec.LongValues[Param1], forward),
                TypeDouble => indexSearcher.GreaterThanQuery(fieldMeta, exec.DoubleValues[Param1], forward),
                _ => indexSearcher.GreaterThanQuerySlice(fieldMeta, exec.GetAnalyzedSlice(indexSearcher, fieldMeta, Param1), forward)
            },
            ClauseType.GreaterThanOrEqual => ValueType switch
            {
                TypeLong => indexSearcher.GreaterThanOrEqualsQuery(fieldMeta, exec.LongValues[Param1], forward),
                TypeDouble => indexSearcher.GreaterThanOrEqualsQuery(fieldMeta, exec.DoubleValues[Param1], forward),
                _ => indexSearcher.GreaterThanOrEqualsQuerySlice(fieldMeta, exec.GetAnalyzedSlice(indexSearcher, fieldMeta, Param1), forward)
            },
            ClauseType.LessThan => ValueType switch
            {
                TypeLong => indexSearcher.LessThanQuery(fieldMeta, exec.LongValues[Param1], forward),
                TypeDouble => indexSearcher.LessThanQuery(fieldMeta, exec.DoubleValues[Param1], forward),
                _ => indexSearcher.LessThanQuerySlice(fieldMeta, exec.GetAnalyzedSlice(indexSearcher, fieldMeta, Param1), forward)
            },
            ClauseType.LessThanOrEqual => ValueType switch
            {
                TypeLong => indexSearcher.LessThanOrEqualsQuery(fieldMeta, exec.LongValues[Param1], forward),
                TypeDouble => indexSearcher.LessThanOrEqualsQuery(fieldMeta, exec.DoubleValues[Param1], forward),
                _ => indexSearcher.LessThanOrEqualsQuerySlice(fieldMeta, exec.GetAnalyzedSlice(indexSearcher, fieldMeta, Param1), forward)
            },
            _ => throw new InvalidOperationException($"RangeQuery does not handle {op}")
        };
    }

    public IQueryMatch BetweenQuery(FieldMetadata fieldMeta, IndexSearcher indexSearcher, QueryExecution exec, bool forward = true)
    {
        return ValueType switch
        {
            TypeLong => indexSearcher.BetweenQuery(fieldMeta, exec.LongValues[Param1], exec.LongValues[Param2], forward: forward),
            TypeDouble => indexSearcher.BetweenQuery(fieldMeta, exec.DoubleValues[Param1], exec.DoubleValues[Param2], forward: forward),
            _ => indexSearcher.BetweenQuerySlice(
                fieldMeta,
                exec.GetAnalyzedSlice(indexSearcher, fieldMeta, Param1),
                exec.GetAnalyzedSlice(indexSearcher, fieldMeta, Param2),
                forward)
        };
    }

    private static void ThrowLimitExceeded(int index)
    {
        throw new InvalidOperationException(
            $"Query parameter index {index} exceeds maximum ({MaxIndex}). " +
            "Simplify the query or reduce the number of IN terms.");
    }
}
