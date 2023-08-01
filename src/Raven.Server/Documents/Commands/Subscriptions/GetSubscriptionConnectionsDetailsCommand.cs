using System;
using System.Net.Http;
using Raven.Client.Http;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.Json;
using Sparrow.Json;

namespace Raven.Server.Documents.Commands.Subscriptions;

public sealed class GetSubscriptionConnectionsDetailsCommand : RavenCommand<SubscriptionConnectionsDetails>
{
    private readonly string _subscriptionName;

    public GetSubscriptionConnectionsDetailsCommand(string subscriptionName, string nodeTag)
    {
        _subscriptionName = subscriptionName;
        SelectedNodeTag = nodeTag;
    }

    public override bool IsReadRequest => false;

    public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
    {
        url = $"{node.Url}/databases/{node.Database}/subscriptions/connection-details";

        if (string.IsNullOrEmpty(_subscriptionName) == false)
            url += $"?name={Uri.EscapeDataString(_subscriptionName)}";

        return new HttpRequestMessage
        {
            Method = HttpMethod.Get
        };
    }

    public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
    {
        if (response == null)
            return;

        Result = JsonDeserializationServer.SubscriptionConnectionsDetails(response);
    }
}
