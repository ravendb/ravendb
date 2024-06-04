using System.Net.Http;
using System.Threading.Tasks;
using Raven.Client.Exceptions;
using Sparrow.Json;

namespace Raven.Client.Http.Behaviors;

internal sealed class DefaultCommandResponseBehavior : AbstractCommandResponseBehavior
{
    public static DefaultCommandResponseBehavior Instance = new();

    private DefaultCommandResponseBehavior()
    {
    }

    public override ValueTask HandleNotModifiedAsync<TResult>(JsonOperationContext context, RavenCommand<TResult> command, HttpResponseMessage response, BlittableJsonReaderObject cachedValue)
    {
        if (command.ResponseType == RavenCommandResponseType.Object)
            command.SetResponse(context, cachedValue, fromCache: true);

        return new ValueTask();
    }

    public override ValueTask<bool> TryHandleNotFoundAsync<TResult>(JsonOperationContext context, RavenCommand<TResult> command, HttpResponseMessage response)
    {
        switch (command.ResponseType)
        {
            case RavenCommandResponseType.Empty:
                break;
            case RavenCommandResponseType.Object:
                command.SetResponse(context, null, fromCache: false);
                break;
            case RavenCommandResponseType.Raw:
            default:
                command.SetResponseRaw(response, null, context);
                break;
        }

        return new ValueTask<bool>(true);
    }

    public override async ValueTask<bool> TryHandleConflictAsync<TResult>(JsonOperationContext context, RavenCommand<TResult> command, HttpResponseMessage response)
    {
        command.OnResponseFailure(response);

        await ExceptionDispatcher.Throw(context, response, unsuccessfulResponseBehavior: CommandUnsuccessfulResponseBehavior.WrapException).ConfigureAwait(false);
        return false;
    }

    public override async ValueTask<bool> TryHandleUnsuccessfulResponseAsync<TResult>(JsonOperationContext context, RavenCommand<TResult> command, HttpResponseMessage response, CommandUnsuccessfulResponseBehavior unsuccessfulResponseBehavior)
    {
        command.OnResponseFailure(response);

        await ExceptionDispatcher.Throw(context, response, unsuccessfulResponseBehavior).ConfigureAwait(false);
        return false;
    }
}
