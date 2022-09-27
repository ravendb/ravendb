using System;
using System.Net.Http;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Microsoft.AspNetCore.Http;
using Raven.Client.Http;
using Raven.Server.Utils;
using Raven.Server.Web.Http.Behaviors;
using Sparrow.Json;

namespace Raven.Server.Web.Http;

public class ProxyCommand : ProxyCommand<object>
{
    public ProxyCommand(RavenCommand command, [NotNull] HttpResponse response) : base(command, response)
    {
    }
}

public class ProxyCommand<T> : RavenCommand
{
    private readonly RavenCommand<T> _command;
    private readonly HttpResponse _response;

    public ProxyCommand(RavenCommand<T> command, [NotNull] HttpResponse response)
    {
        _command = command ?? throw new ArgumentNullException(nameof(command));
        _response = response ?? throw new ArgumentNullException(nameof(response));
        ResponseBehavior = new ProxyCommandResponseBehavior(response);
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
