using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Serialization;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.CdcSink;

public class AddCdcSinkOperation : IMaintenanceOperation<AddCdcSinkOperationResult>
{
    private readonly CdcSinkConfiguration _configuration;

    public AddCdcSinkOperation(CdcSinkConfiguration configuration)
    {
        _configuration = configuration;
    }

    public RavenCommand<AddCdcSinkOperationResult> GetCommand(DocumentConventions conventions, JsonOperationContext ctx)
    {
        return new AddCdcSinkCommand(conventions, _configuration);
    }

    private class AddCdcSinkCommand : RavenCommand<AddCdcSinkOperationResult>, IRaftCommand
    {
        private readonly CdcSinkConfiguration _configuration;
        private readonly DocumentConventions _conventions;

        public AddCdcSinkCommand(DocumentConventions conventions, CdcSinkConfiguration configuration)
        {
            _conventions = conventions ?? throw new ArgumentNullException(nameof(conventions));
            _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
        }

        public override bool IsReadRequest => false;

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/admin/cdc-sink";

            return new HttpRequestMessage
            {
                Method = HttpMethod.Put,
                Content = new BlittableJsonContent(
                    async stream => await ctx.WriteAsync(stream, DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(_configuration, ctx)).ConfigureAwait(false),
                    _conventions)
            };
        }

        public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
        {
            if (response == null)
                ThrowInvalidResponse();

            Result = JsonDeserializationClient.AddCdcSinkOperationResult(response);
        }

        public string RaftUniqueRequestId { get; } = RaftIdGenerator.NewId();
    }
}

public class AddCdcSinkOperationResult
{
    public long RaftCommandIndex { get; set; }
    public long TaskId { get; set; }
}
