using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Includes;

public interface ITimeSeriesIncludes
{
    ValueTask<int> WriteIncludesAsync(AsyncBlittableJsonTextWriter writer, JsonOperationContext context, CancellationToken token);

    int Count { get; }

    public void Gather(List<BlittableJsonReaderObject> list, ClusterOperationContext clusterOperationContext);

    long GetEntriesCountForStats();

    void Fill(Document resultDoc);

    bool HasEntries();
}
