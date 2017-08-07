using System;

namespace Raven.Client.Documents.Queries.Facets
{
    public enum FacetMode
    {
        Default,
        Ranges
    }

    [Flags]
    public enum FacetAggregation
    {
        None = 0,
        Count = 1,
        Max = 2,
        Min = 4,
        Average = 8,
        Sum = 16
    }
}
