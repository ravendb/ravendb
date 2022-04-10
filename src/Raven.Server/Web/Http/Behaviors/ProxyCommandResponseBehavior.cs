using System;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Microsoft.AspNetCore.Http;
using Raven.Client.Http;
using Raven.Client.Http.Behaviors;
using Raven.Server.Utils;
using Sparrow.Json;

namespace Raven.Server.Web.Http.Behaviors;

internal class ProxyCommandResponseBehavior : AbstractCommandResponseBehavior
{
    private readonly HttpResponse _response;

    public ProxyCommandResponseBehavior([NotNull] HttpResponse response)
    {
        _response = response ?? throw new ArgumentNullException(nameof(response));
    }

    public override ValueTask HandleNotModifiedAsync<TResult>(JsonOperationContext context, RavenCommand<TResult> command, HttpResponseMessage response, BlittableJsonReaderObject cachedValue)
    {
        HttpResponseHelper.CopyStatusCode(response, _response);
        HttpResponseHelper.CopyHeaders(response, _response);

        return ValueTask.CompletedTask;
    }

    public override ValueTask<bool> TryHandleNotFoundAsync<TResult>(JsonOperationContext context, RavenCommand<TResult> command, HttpResponseMessage response)
    {
        HttpResponseHelper.CopyStatusCode(response, _response);
        HttpResponseHelper.CopyHeaders(response, _response);

        return ValueTask.FromResult(true);
    }

    public override async ValueTask<bool> TryHandleConflictAsync<TResult>(JsonOperationContext context, RavenCommand<TResult> command, HttpResponseMessage response)
    {
        HttpResponseHelper.CopyStatusCode(response, _response);
        HttpResponseHelper.CopyHeaders(response, _response);
        await HttpResponseHelper.CopyContentAsync(response, _response).ConfigureAwait(false);

        return true;
    }

    public override async ValueTask<bool> TryHandleUnsuccessfulResponseAsync<TResult>(JsonOperationContext context, RavenCommand<TResult> command, HttpResponseMessage response)
    {
        HttpResponseHelper.CopyStatusCode(response, _response);
        HttpResponseHelper.CopyHeaders(response, _response);
        await HttpResponseHelper.CopyContentAsync(response, _response).ConfigureAwait(false);

        return true;
    }
}
