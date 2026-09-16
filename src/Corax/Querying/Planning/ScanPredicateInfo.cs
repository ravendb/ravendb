namespace Corax.Querying.Planning;

public enum GroupKind : byte
{
    And,
    Or
}

public struct ScanPredicateInfo
{
    public string FieldName;
    public ScanValueType ValueType;
    public ScanCompareOp CompareOp;
    public int ParamIndex;
    public int ParamIndex2;
    public ScanPredicateInfo[] SubPredicates;
    public GroupKind Group;
    public bool Negated;
    /// <summary>If we _know_ that the field has a single value, we can avoid a while loop to read mutliple values</summary>
    public bool IsSingleValued;
    public bool IncludeNull;
}
