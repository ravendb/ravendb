using System.Threading;
using System.Threading.Tasks;
using Sparrow.Json;

namespace Raven.Server.Documents.Includes;

public abstract class AbstractIncludeTimeSeriesCommand : ITimeSeriesIncludes
{
    public abstract ValueTask<int> WriteIncludesAsync(AsyncBlittableJsonTextWriter writer, JsonOperationContext context, CancellationToken token);

    public abstract int Count { get; }

    public abstract long GetEntriesCountForStats();
}
