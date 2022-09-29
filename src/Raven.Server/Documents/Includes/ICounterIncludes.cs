using System.Threading.Tasks;
using System.Threading;
using Sparrow.Json;
using System.Collections.Generic;

namespace Raven.Server.Documents.Includes;

public interface ICounterIncludes
{
    ValueTask WriteIncludesAsync(AsyncBlittableJsonTextWriter writer, JsonOperationContext context, CancellationToken token);

    Dictionary<string, string[]> IncludedCounterNames { get; }

    int Count { get; }
}
