using System.Threading;
using System.Threading.Tasks;
using Raven.Server.Json;
using Sparrow.Json;

namespace Raven.Server.Documents.Includes.Sharding;

public sealed class OrchestratorIncludesCommandImpl : AbstractIncludesCommand
{
    public readonly IncludeDocumentsOrchestratedSubscriptionCommand IncludeDocumentsCommand;
    public readonly ShardedTimeSeriesIncludes IncludeTimeSeriesCommand;
    public readonly ShardedCounterIncludes IncludeCountersCommand;

    public OrchestratorIncludesCommandImpl(IncludeDocumentsOrchestratedSubscriptionCommand includeDocumentsCommand, ShardedTimeSeriesIncludes includeTimeSeriesCommand, ShardedCounterIncludes includeCountersCommand) 
        : base(includeDocumentsCommand, includeTimeSeriesCommand, includeCountersCommand)
    {
        IncludeDocumentsCommand = includeDocumentsCommand;
        IncludeTimeSeriesCommand = includeTimeSeriesCommand;
        IncludeCountersCommand = includeCountersCommand;
    }

    protected override ValueTask<(long count, long sizeInBytes)> WriteIncludedDocumentsInternalAsync(AsyncBlittableJsonTextWriter writer, JsonOperationContext context,
        CancellationToken token)
    {
        return writer.WriteIncludesAsync(IncludeDocumentsCommand.Includes, token);
    }
}
