using System;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client;
using Raven.Client.Documents.Operations;
using Raven.Client.Http;
using Raven.Client.Json.Serialization;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Server.Web.Http;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.OngoingTasks
{
    internal class OngoingTasksHandlerProcessorForGetRunningBackupOperationStatus : AbstractDatabaseHandlerProcessor<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public OngoingTasksHandlerProcessorForGetRunningBackupOperationStatus([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        public override async ValueTask ExecuteAsync()
        {
            var taskId = RequestHandler.GetLongQueryString("taskId");
            
            var responsibleNode = await BackupUtils.WaitAndGetResponsibleNodeAsync(taskId, RequestHandler.Database);

            if (responsibleNode == ServerStore.NodeTag)
            {
                var backup = RequestHandler.Database.PeriodicBackupRunner.PeriodicBackups.FirstOrDefault(x => x.Configuration.TaskId == taskId);
                if (backup == null)
                {
                    throw new InvalidOperationException($"Backup task id: {taskId} doesn't exist");
                }

                var result = new OperationIdResult()
                {
                    OperationNodeTag = responsibleNode,
                    OperationId = backup.RunningTask?.Id ?? Constants.Operations.InvalidOperationId
                };
                
                using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
                await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
                {
                    writer.WriteStartObject();
                    writer.WritePropertyName(nameof(OperationIdResult.OperationNodeTag));
                    writer.WriteString(result.OperationNodeTag);
                    writer.WriteComma();
                    writer.WritePropertyName(nameof(OperationIdResult.OperationId));
                    writer.WriteInteger(result.OperationId);
                    writer.WriteEndObject();
                }
            }
            else
            {
                //redirect
                var cmd = new GetRunningBackupStatusCommand(taskId);
                cmd.SelectedNodeTag = responsibleNode;
                await RequestHandler.ExecuteRemoteAsync(new ProxyCommand<OperationIdResult>(cmd, HttpContext.Response));
            }
        }
    }

    internal sealed class GetRunningBackupStatusCommand : RavenCommand<OperationIdResult>
    {
        public override bool IsReadRequest => true;

        private readonly long _taskId;

        public GetRunningBackupStatusCommand(long taskId)
        {
            _taskId = taskId;
        }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/admin/backup/running?taskId={_taskId}";

            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Get
            };

            return request;
        }

        public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
        {
            if (response == null)
                ThrowInvalidResponse();

            var result = JsonDeserializationClient.OperationIdResult(response);
            Result = result;
        }
    }
}
