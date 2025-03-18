namespace Raven.Client.Documents.Session.Tokens
{
    public enum WhereOperator
    {
        Equals,
        NotEquals,
        GreaterThan,
        GreaterThanOrEqual,
        LessThan,
        LessThanOrEqual,
        In,
        AllIn,
        Between,
        Search,
        Lucene,
        StartsWith,
        EndsWith,
        Exists,
        Spatial_Within,
        Spatial_Contains,
        Spatial_Disjoint,
        Spatial_Intersects,
        Regex,
        Vector_Search
    }
}
