using System.Collections.Generic;
using System.Net.Http;
using Microsoft.AspNetCore.WebUtilities;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Server.Documents.Commands.Notifications;

public sealed class GetDatabaseNotificationsCommand : RavenCommand<BlittableJsonReaderObject>
{
    public GetDatabaseNotificationsCommand(bool postponed, string type, int start, int pageSize, string nodeTag = null)
    {
        SelectedNodeTag = nodeTag;
        Postponed = postponed;
        Type = type;
        Start = start;
        PageSize = pageSize;
    }
        
    public override bool IsReadRequest => true;

    private bool Postponed { get; }
    private string Type { get; }
    private int Start { get; }
    private int PageSize { get; }
    
    public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
    {
        var baseUrl = $"{node.Url}/databases/{node.Database}/notifications";
        
        var queryParams = new Dictionary<string, string>
        {
            { nameof(Postponed).ToLowerInvariant(), Postponed.ToString() },
            { nameof(PageSize).ToLowerInvariant(), PageSize.ToString() },
            { nameof(Type).ToLowerInvariant(), Type },
            { nameof(Start).ToLowerInvariant(), Start.ToString() },
        };

        url = QueryHelpers.AddQueryString(baseUrl, queryParams);

        return new HttpRequestMessage
        {
            Method = HttpMethod.Get
        };
    }
        
    public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
    {
        if (response == null)
            ThrowInvalidResponse();

        Result = response;
    }
}
