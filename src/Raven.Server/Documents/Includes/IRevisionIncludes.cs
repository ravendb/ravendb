using System.Threading.Tasks;
using System.Threading;
using Sparrow.Json;

namespace Raven.Server.Documents.Includes;

public interface IRevisionIncludes
{
    ValueTask WriteIncludesAsync(AsyncBlittableJsonTextWriter writer, JsonOperationContext context, CancellationToken token);

    int Count { get; }
}
