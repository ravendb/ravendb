using System;
using System.Net.Http;
using JetBrains.Annotations;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Server.NotificationCenter.Commands;

internal class PostponeNotificationCommand : RavenCommand
{
    private readonly string _id;
    private readonly long _timeInSec;

    public PostponeNotificationCommand([NotNull] string id, long timeInSec, string nodeTag = null)
    {
        _id = id ?? throw new ArgumentNullException(nameof(id));
        _timeInSec = timeInSec;
        SelectedNodeTag = nodeTag;
    }

    public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
    {
        url = $"{node.Url}/databases/{node.Database}/notification-center/postpone?id={Uri.EscapeDataString(_id)}&timeInSec={_timeInSec}";

        return new HttpRequestMessage
        {
            Method = HttpMethod.Post
        };
    }
}
