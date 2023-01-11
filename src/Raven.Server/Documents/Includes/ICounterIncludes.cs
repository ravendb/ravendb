using System.Threading.Tasks;
using System.Threading;
using Sparrow.Json;
using System.Collections.Generic;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Includes;

public interface ICounterIncludes
{
    ValueTask WriteIncludesAsync(AsyncBlittableJsonTextWriter writer, JsonOperationContext context, CancellationToken token);

    Dictionary<string, string[]> IncludedCounterNames { get; }

    int Count { get; }

    public void Fill(Document document);

    public void Gather(List<(BlittableJsonReaderObject Includes, Dictionary<string, string[]> IncludedCounterNames)> list, ClusterOperationContext clusterOperationContext);

    long GetCountersSize();

    long GetCountersCount();

    bool HasCountersIncludes();
}
