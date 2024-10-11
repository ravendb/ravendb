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
        Fuzzy,
        Proximity,

        Count,
        Sum,
        Min,
        Max,
        Average,

        Last,

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

        Highlight,

        Explanation,

        Timings,

        Counters,
        
        Revisions,

        TimeSeries,

        Unknown,
        
        Vector_Search,
        Embedding_Text, //text
        Embedding_Text_I8, // text_i8
        Embedding_Text_I1,
        Embedding_F32_I8,
        Embedding_F32_I1,
        Embedding_F32,
        Embedding_I8,
        Embedding_I1,
    }
}
