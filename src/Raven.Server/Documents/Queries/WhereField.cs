using Raven.Client.Documents.Indexes.Spatial;
using Raven.Client.Documents.Indexes.Vector;

namespace Raven.Server.Documents.Queries
{
    public sealed class WhereField(bool isFullTextSearch, bool isExactSearch, AutoSpatialOptions spatial, AutoVectorOptions vector)
    {
        public readonly AutoSpatialOptions Spatial = spatial;
        public readonly AutoVectorOptions Vector = vector;

        public readonly bool IsFullTextSearch = isFullTextSearch;
        public readonly bool IsExactSearch = isExactSearch;
    }
}
