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

        Unknown,
    }
}
