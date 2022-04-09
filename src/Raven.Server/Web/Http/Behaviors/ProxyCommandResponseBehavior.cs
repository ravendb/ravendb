using System;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Microsoft.AspNetCore.Http;
using Raven.Client.Http;
using Raven.Client.Http.Behaviors;
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
        _response.StatusCode = (int)HttpStatusCode.NotModified;

        return ValueTask.CompletedTask;
    }

    public override ValueTask<bool> TryHandleNotFoundAsync<TResult>(JsonOperationContext context, RavenCommand<TResult> command, HttpResponseMessage response)
    {
        _response.StatusCode = (int)HttpStatusCode.NotFound;

        return ValueTask.FromResult(true);
    }

    public override async ValueTask<bool> TryHandleConflictAsync<TResult>(JsonOperationContext context, RavenCommand<TResult> command, HttpResponseMessage response)
    {
        await CopyResponseAsync(response);
        return true;
    }

    public override async ValueTask<bool> TryHandleUnsuccessfulResponseAsync<TResult>(JsonOperationContext context, RavenCommand<TResult> command, HttpResponseMessage response)
    {
        await CopyResponseAsync(response);
        return true;
    }

    private async ValueTask CopyResponseAsync(HttpResponseMessage response)
    {
        _response.StatusCode = (int)response.StatusCode;

        foreach (var header in response.Headers)
            _response.Headers.Add(header.Key, header.Value.ToArray());

        await response.Content.CopyToAsync(_response.Body);
    }
}
