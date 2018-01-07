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
        Intersect,

        Count,
        Sum,
        Min,
        Max,
        Average,

        CompareExchange,
        
        Spatial_Circle,
        Spatial_Wkt,
        Spatial_Point,
        Spatial_Within,
        Spatial_Contains,
        Spatial_Disjoint,
        Spatial_Intersects,

        MoreLikeThis,

        Array,

        Unknown,
    }
}
