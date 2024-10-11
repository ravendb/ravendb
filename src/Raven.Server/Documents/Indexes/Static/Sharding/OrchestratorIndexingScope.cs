using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Indexes.Static.Sharding;

public sealed class OrchestratorIndexingScope : CurrentIndexingScope
{
    public OrchestratorIndexingScope(TransactionOperationContext context, UnmanagedBuffersPoolWithLowMemoryHandling unmanagedBuffersPool) 
        : base(null, null, null, null, context, null, null, unmanagedBuffersPool)
    {
    }

    public override bool SupportsDynamicFieldsCreation => false;

    public override bool SupportsSpatialFieldsCreation => false;
}
