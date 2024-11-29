using Sparrow.Server;

namespace Voron.Data.Graphs;

public partial class Hnsw
{
    public const long EntryPointId = 1;
    
    private static readonly Slice VectorsContainerIdSlice;
    private static readonly Slice NodeIdToLocationSlice;
    private static readonly Slice NodesByVectorIdSlice;
    private static readonly Slice VectorsIdByHashSlice;
    private static readonly Slice HnswGlobalConfigSlice;
    internal static readonly Slice OptionsSlice;
    
    static Hnsw()
    {
        using (StorageEnvironment.GetStaticContext(out var ctx))
        {
            // Global to all HNSWs
            Slice.From(ctx, "VectorsContainerId", ByteStringType.Immutable, out VectorsContainerIdSlice);
            Slice.From(ctx, "HnswGlobalConfig", ByteStringType.Immutable, out HnswGlobalConfigSlice);
            Slice.From(ctx, "VectorsIdByHash", ByteStringType.Immutable, out VectorsIdByHashSlice);
            // Local to a single HNSW
            Slice.From(ctx, "NodeIdToLocation", ByteStringType.Immutable, out NodeIdToLocationSlice);
            Slice.From(ctx, "NodesByVectorId", ByteStringType.Immutable, out NodesByVectorIdSlice);
            Slice.From(ctx, "Options", ByteStringType.Immutable, out OptionsSlice);
        }
    }
}
