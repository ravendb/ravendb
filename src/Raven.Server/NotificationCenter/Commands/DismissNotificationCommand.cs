using System;
using System.Net.Http;
using JetBrains.Annotations;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Server.NotificationCenter.Commands;

internal class DismissNotificationCommand : RavenCommand
{
    private readonly string _id;
    private readonly bool _forever;

    public DismissNotificationCommand([NotNull] string id, bool forever = false, string nodeTag = null)
    {
        _id = id ?? throw new ArgumentNullException(nameof(id));
        _forever = forever;
        SelectedNodeTag = nodeTag;
    }

    public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
    {
        url = $"{node.Url}/databases/{node.Database}/notification-center/dismiss?id={Uri.EscapeDataString(_id)}";

        if (_forever)
            url += $"&forever={_forever}";

        return new HttpRequestMessage
        {
            Method = HttpMethod.Post
        };
    }
}
