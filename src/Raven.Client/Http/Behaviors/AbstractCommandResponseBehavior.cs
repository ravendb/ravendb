using System.Net.Http;
using System.Threading.Tasks;
using Sparrow.Json;

namespace Raven.Client.Http.Behaviors;

internal abstract class AbstractCommandResponseBehavior
{
    public abstract ValueTask HandleNotModifiedAsync<TResult>(JsonOperationContext context, RavenCommand<TResult> command, HttpResponseMessage response, BlittableJsonReaderObject cachedValue);

    public abstract ValueTask<bool> TryHandleNotFoundAsync<TResult>(JsonOperationContext context, RavenCommand<TResult> command, HttpResponseMessage response);

    public abstract ValueTask<bool> TryHandleConflictAsync<TResult>(JsonOperationContext context, RavenCommand<TResult> command, HttpResponseMessage response);

    public abstract ValueTask<bool> TryHandleUnsuccessfulResponseAsync<TResult>(JsonOperationContext context, RavenCommand<TResult> command, HttpResponseMessage response);
}
