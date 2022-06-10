using System;
using System.Collections.Generic;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Util;
using Raven.Server.Documents.Queries;
using Raven.Server.Json;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Commands.Queries;

public class PatchByQueryTestCommand : RavenCommand<PatchByQueryTestCommand.Response>
{
    private readonly string _id;
    private readonly IndexQueryServerSide _query;

    public class Response : PatchResult
    {
        public List<string> Output { get; set; }

        public BlittableJsonReaderObject DebugActions { get; set; }
    }

    public PatchByQueryTestCommand(string id, IndexQueryServerSide query)
    {
        _id = id ?? throw new ArgumentNullException(nameof(id));
        _query = query ?? throw new ArgumentNullException(nameof(query));
    }

    public override bool IsReadRequest => true;

    public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
    {
        url = $"{node.Url}/databases/{node.Database}/queries/test?id={Uri.EscapeDataString(_id)}";

        return new HttpRequestMessage
        {
            Method = HttpMethods.Patch,
            Content = new BlittableJsonContent(async stream =>
            {
                await using (var writer = new AsyncBlittableJsonTextWriter(ctx, stream))
                {
                    writer.WriteStartObject();

                    writer.WritePropertyName("Query");
                    writer.WriteIndexQuery(ctx, _query);

                    writer.WriteEndObject();
                }
            })
        };
    }

    public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
    {
        if (response == null)
            ThrowInvalidResponse();

        Result = DocumentConventions.DefaultForServer.Serialization.DefaultConverter.FromBlittable<Response>(response);
    }
}
