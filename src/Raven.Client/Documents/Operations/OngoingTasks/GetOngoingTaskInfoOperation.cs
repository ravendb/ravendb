using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json.Converters;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.OngoingTasks
{
    public class GetOngoingTaskInfoOperation : IMaintenanceOperation<OngoingTask>
    {
        private readonly long _taskId;
        private readonly OngoingTaskType _type;

        public GetOngoingTaskInfoOperation(long taskId, OngoingTaskType type)
        {
            _taskId = taskId;
            _type = type;
        }

        public RavenCommand<OngoingTask> GetCommand(DocumentConventions conventions, JsonOperationContext ctx)
        {
            return new GetOngoingTaskInfoCommand(_taskId, _type);
        }

        private class GetOngoingTaskInfoCommand : RavenCommand<OngoingTask>
        {
            private readonly long _taskId;
            private readonly OngoingTaskType _type;

            public GetOngoingTaskInfoCommand(long taskId, OngoingTaskType type)
            {
                _taskId = taskId;
                _type = type;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/task?key={_taskId}&type={_type}";

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Get
                };

                return request;
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if (response != null)
                {
                    switch (_type)
                    {
                        case OngoingTaskType.Replication:
                            Result = JsonDeserializationClient.GetOngoingTaskReplicationResult(response);
                            break;
                        case OngoingTaskType.RavenEtl:
                            Result = JsonDeserializationClient.GetOngoingTaskRavenEtlResult(response);
                            break;
                        case OngoingTaskType.SqlEtl:
                            Result = JsonDeserializationClient.GetOngoingTaskSqlEtlResult(response);
                            break;
                        case OngoingTaskType.Backup:
                            Result = JsonDeserializationClient.GetOngoingTaskBackupResult(response);
                            break;
                        case OngoingTaskType.Subscription:
                            Result = JsonDeserializationClient.GetOngoingTaskSubscriptionResult(response);
                            break;
                        case OngoingTaskType.PullReplicationAsHub:
                            Result = JsonDeserializationClient.OngoingTaskPullReplicationAsHubResult(response);
                            break;
                        case OngoingTaskType.PullReplicationAsSink:
                            Result = JsonDeserializationClient.OngoingTaskPullReplicationAsSinkResult(response);
                            break;
                        default:
                            throw new ArgumentOutOfRangeException();
                    }
                }
            }

            public override bool IsReadRequest => false;
        }
    }
}
