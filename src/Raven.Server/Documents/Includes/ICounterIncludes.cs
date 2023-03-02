using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Sparrow.Json;

namespace Raven.Server.Documents.Includes;

public interface ICounterIncludes
{
    ValueTask WriteIncludesAsync(AsyncBlittableJsonTextWriter writer, JsonOperationContext context, CancellationToken token);

    Dictionary<string, string[]> IncludedCounterNames { get; }

    int Count { get; }

    long GetCountersSize();

    long GetCountersCount();
}
