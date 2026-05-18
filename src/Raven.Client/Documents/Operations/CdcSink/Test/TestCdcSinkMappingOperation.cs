using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Serialization;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.CdcSink.Test;

/// <summary>
/// Drives a synthetic CDC mapping run against one or more rows of the configured source
/// table — used to preview how rows will become documents before saving a CDC task.
/// Calls <c>POST /admin/cdc-sink/test</c>. Requires <c>DatabaseAdmin</c>.
/// </summary>
public class TestCdcSinkMappingOperation : IMaintenanceOperation<TestCdcSinkMappingResult>
{
    private readonly TestCdcSinkMappingRequest _request;

    public TestCdcSinkMappingOperation(TestCdcSinkMappingRequest request)
    {
        _request = request ?? throw new ArgumentNullException(nameof(request));
    }

    public RavenCommand<TestCdcSinkMappingResult> GetCommand(DocumentConventions conventions, JsonOperationContext ctx)
    {
        return new TestCdcSinkMappingCommand(conventions, _request);
    }

    private sealed class TestCdcSinkMappingCommand : RavenCommand<TestCdcSinkMappingResult>
    {
        private readonly TestCdcSinkMappingRequest _request;
        private readonly DocumentConventions _conventions;

        public TestCdcSinkMappingCommand(DocumentConventions conventions, TestCdcSinkMappingRequest request)
        {
            _conventions = conventions ?? throw new ArgumentNullException(nameof(conventions));
            _request = request ?? throw new ArgumentNullException(nameof(request));
        }

        // POST verb but no server-side state change — allow FastestNode failover.
        public override bool IsReadRequest => true;

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/admin/cdc-sink/test";

            return new HttpRequestMessage
            {
                Method = HttpMethod.Post,
                Content = new BlittableJsonContent(
                    async stream => await ctx.WriteAsync(stream, ctx.ReadObject(_request.ToJson(), "TestCdcSinkMappingRequest")).ConfigureAwait(false),
                    _conventions),
            };
        }

        public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
        {
            if (response == null)
                ThrowInvalidResponse();

            Result = JsonDeserializationClient.TestCdcSinkMappingResult(response);
        }
    }
}
