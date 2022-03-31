using System.Collections.Generic;
using System.Net.Http;
using Raven.Client.Http;
using Raven.Client.Json;
using Sparrow.Json;

namespace Raven.Server.Documents.Commands;

internal class WaitForIndexNotificationCommand : RavenCommand
{
    private readonly List<long> _indexes;

    public WaitForIndexNotificationCommand(List<long> indexes)
    {
        _indexes = indexes;
    }

    public override bool IsReadRequest => true;

    public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
    {
        url = $"{node.Url}/databases/{node.Database}/admin/rachis/wait-for-index-notifications";

        var request = new HttpRequestMessage
        {
            Method = HttpMethod.Post,
            Content = new BlittableJsonContent(async stream =>
            {
                await using (var writer = new AsyncBlittableJsonTextWriter(ctx, stream))
                {
                    writer.WriteStartObject();
                    writer.WriteArray(nameof(WaitForIndexNotificationRequest.RaftCommandIndexes), _indexes);
                    writer.WriteEndObject();
                }
            })
        };

        return request;
    }
}
