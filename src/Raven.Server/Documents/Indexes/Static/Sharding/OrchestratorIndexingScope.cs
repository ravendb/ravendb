using Raven.Server.Documents.Indexes.Static.Spatial;

namespace Raven.Server.Documents.Indexes.Static.Sharding;

public class OrchestratorIndexingScope : CurrentIndexingScope
{
    public OrchestratorIndexingScope() : base(null, null, null, null, null, null, null)
    {
    }

    public override SpatialField GetOrCreateSpatialField(string name)
    {
        return null;
    }
}
