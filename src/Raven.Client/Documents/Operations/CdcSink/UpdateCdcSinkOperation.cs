using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Serialization;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.CdcSink;

public class UpdateCdcSinkOperation : IMaintenanceOperation<UpdateCdcSinkOperationResult>
{
    private readonly long _taskId;
    private readonly CdcSinkConfiguration _configuration;

    public UpdateCdcSinkOperation(long taskId, CdcSinkConfiguration configuration)
    {
        _taskId = taskId;
        _configuration = configuration;
    }

    public RavenCommand<UpdateCdcSinkOperationResult> GetCommand(DocumentConventions conventions, JsonOperationContext ctx)
    {
        return new UpdateCdcSinkCommand(conventions, _taskId, _configuration);
    }

    private class UpdateCdcSinkCommand : RavenCommand<UpdateCdcSinkOperationResult>, IRaftCommand
    {
        private readonly long _taskId;
        private readonly CdcSinkConfiguration _configuration;
        private readonly DocumentConventions _conventions;

        public UpdateCdcSinkCommand(DocumentConventions conventions, long taskId, CdcSinkConfiguration configuration)
        {
            _taskId = taskId;
            _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
            _conventions = conventions ?? throw new ArgumentNullException(nameof(conventions));
        }

        public override bool IsReadRequest => false;

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/admin/cdc-sink?id={_taskId}";

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

            Result = JsonDeserializationClient.UpdateCdcSinkOperationResult(response);
        }

        public string RaftUniqueRequestId { get; } = RaftIdGenerator.NewId();
    }
}

public class UpdateCdcSinkOperationResult
{
    public long RaftCommandIndex { get; set; }
    public long TaskId { get; set; }
}
