using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Sparrow.Json;

namespace Raven.Server.Documents.Includes;

public abstract class AbstractIncludeCountersCommand : ICounterIncludes
{
    public abstract ValueTask WriteIncludesAsync(AsyncBlittableJsonTextWriter writer, JsonOperationContext context, CancellationToken token);

    public abstract Dictionary<string, string[]> IncludedCounterNames { get; }

    public abstract int Count { get; }

    public abstract long GetCountersSize();

    public abstract long GetCountersCount();
}
