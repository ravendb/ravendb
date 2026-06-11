using System;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Raven.Client.Http;
using Raven.Server.Utils;
using Raven.Server.Web.Http.Behaviors;
using Sparrow.Json;

namespace Raven.Server.Web.Http;

public sealed class ProxyCommand : ProxyCommand<object>
{
    public ProxyCommand(RavenCommand command, HttpContext httpContext)
        : base(command, httpContext)
    {
    }
}

public class ProxyCommand<T> : RavenCommand
{
    private readonly RavenCommand<T> _command;
    private readonly HttpResponse _response;
    private readonly HttpRequest _request;

    public ProxyCommand(RavenCommand<T> command, HttpContext httpContext)
    {
        _command = command ?? throw new ArgumentNullException(nameof(command));
        _response = httpContext?.Response ?? throw new ArgumentNullException(nameof(httpContext.Response));
        _request = httpContext?.Request ?? throw new ArgumentNullException(nameof(httpContext.Request));
        ResponseBehavior = new ProxyCommandResponseBehavior(_response);
    }

    public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
    {
        return _command.CreateRequest(ctx, node, out url);
    }

    public override async Task<ResponseDisposeHandling> ProcessResponse(JsonOperationContext context, HttpCache cache, HttpResponseMessage response, string url)
    {
        HttpResponseHelper.CopyStatusCode(response, _response);
        HttpResponseHelper.CopyHeaders(response, _response);

        await HttpResponseHelper.CopyContentAsync(response, _response);

        return ResponseDisposeHandling.Automatic;
    }

    public override void OnBeforeRequest(HttpRequestMessage request)
    {
        if (_request == null)
            return;

        if (_request.Headers.AcceptEncoding.Count == 0)
            return;

        if (_request.Headers.AcceptEncoding == request.Headers.AcceptEncoding)
            return;

        request.Headers.AcceptEncoding.Clear();
        foreach (var encoding in _request.Headers.AcceptEncoding)
            request.Headers.AcceptEncoding.ParseAdd(encoding);
    }

    public override bool IsReadRequest => _command.IsReadRequest;

    public override bool CanCache
    {
        get => _command?.CanCache ?? false;
        protected internal set
        {
            if (_command != null)
                _command.CanCache = value;
        }
    }

    public override bool CanCacheAggressively
    {
        get => _command?.CanCacheAggressively ?? false;
        protected internal set
        {
            if (_command != null)
                _command.CanCacheAggressively = value;
        }
    }

    internal override bool CanReadFromCache
    {
        get => _command?.CanReadFromCache ?? false;
        set
        {
            if (_command != null)
                _command.CanReadFromCache = value;
        }
    }

    public override RavenCommandResponseType ResponseType
    {
        get => _command?.ResponseType ?? RavenCommandResponseType.Empty;
        protected internal set
        {
            if (_command != null)
                _command.ResponseType = value;
        }
    }

    public override string SelectedNodeTag
    {
        get => _command.SelectedNodeTag;
        protected internal set => _command.SelectedNodeTag = value;
    }

    public override TimeSpan? Timeout
    {
        get => _command.Timeout;
        protected internal set => _command.Timeout = value;
    }
}
