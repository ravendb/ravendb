using Raven.Client.Documents.Indexes.Spatial;

namespace Raven.Server.Documents.Queries
{
    public sealed class WhereField
    {
        public readonly AutoSpatialOptions Spatial;

        public readonly bool IsFullTextSearch;

        public readonly bool IsExactSearch;

        public bool IsVector;

        public WhereField(bool isFullTextSearch, bool isExactSearch, AutoSpatialOptions spatial)
        {
            Spatial = spatial;
            IsFullTextSearch = isFullTextSearch;
            IsExactSearch = isExactSearch;
        }
    }
}
