namespace Raven.Server.Documents.Queries
{
    public enum MethodType
    {
        Id,
        Search,
        Boost,
        Regex,
        StartsWith,
        EndsWith,
        Lucene,
        Exists,
        Exact,
        Count,
        Sum,
        Average,
        Intersect,

        CmpXchg,
        
        Circle,
        Wkt,
        Point,
        Within,
        Contains,
        Disjoint,
        Intersects,

        MoreLikeThis,

        Array,

        Unknown,
    }
}
