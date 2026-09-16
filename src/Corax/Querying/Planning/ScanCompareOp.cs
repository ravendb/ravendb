using System;

namespace Corax.Querying.Planning;

public enum ScanCompareOp : byte
{
    Equals,
    NotEquals,
    GreaterThan,
    GreaterThanOrEqual,
    LessThan,
    LessThanOrEqual,
    Between,
    /// <summary>Field exists — reader.FindNext succeeds. No value comparison.</summary>
    Exists,
    /// <summary>StartsWith — reader term starts with the param slice. Iterate all terms for multi-value fields.</summary>
    StartsWith,
    /// <summary>EndsWith — reader term ends with the param slice.</summary>
    EndsWith,
    /// <summary>IN — the field has at least one term equal to one of the IN value set (OR semantics).</summary>
    In,
    /// <summary>ALL IN — the (multi-valued) field contains every value in the set (set-containment).</summary>
    AllIn,
    AlwaysTrue,
    AlwaysFalse,
}

public static class ScanCompareOpsHelper
{
    public static string ToOperator(string s)
    {
        return Enum.TryParse<ScanCompareOp>(s, out var op) ? op.ToOperator() : s;
    } 
    
    public static string ToOperator(this ScanCompareOp op)
    {
        return op switch
        {
            ScanCompareOp.Equals             => "==",
            ScanCompareOp.NotEquals          => "!=",
            ScanCompareOp.GreaterThan        => ">",
            ScanCompareOp.GreaterThanOrEqual => ">=",
            ScanCompareOp.LessThan           => "<",
            ScanCompareOp.LessThanOrEqual    => "<=",
            ScanCompareOp.Between            => "BETWEEN",
            ScanCompareOp.Exists             => "EXISTS",
            ScanCompareOp.StartsWith         => "STARTS WITH",
            ScanCompareOp.EndsWith           => "ENDS WITH",
            ScanCompareOp.In                 => "IN",
            ScanCompareOp.AllIn              => "ALL IN",
            _                                => op.ToString() 
        };
    }
}
