using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json.Serialization;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.OngoingTasks
{
    public sealed class GetOngoingTaskInfoOperation : IMaintenanceOperation<OngoingTask>
    {
        private readonly string _taskName;
        private readonly long _taskId;
        private readonly OngoingTaskType _type;

        public GetOngoingTaskInfoOperation(long taskId, OngoingTaskType type)
        {
            _taskId = taskId;
            _type = type;

            if (type == OngoingTaskType.PullReplicationAsHub)
            {
                throw new ArgumentException(nameof(OngoingTaskType.PullReplicationAsHub) + " type is not supported. Please use " + nameof(GetPullReplicationTasksInfoOperation) + " instead.");
            }
        }

        public GetOngoingTaskInfoOperation(string taskName, OngoingTaskType type)
        {
            _taskName = taskName;
            _type = type;
        }

        public RavenCommand<OngoingTask> GetCommand(DocumentConventions conventions, JsonOperationContext ctx)
        {
            if (_taskName != null)
                return new GetOngoingTaskInfoCommand(_taskName, _type);
            return new GetOngoingTaskInfoCommand(_taskId, _type);
        }

        internal sealed class GetOngoingTaskInfoCommand : RavenCommand<OngoingTask>
        {
            private readonly string _taskName;
            private readonly long _taskId;
            private readonly OngoingTaskType _type;

            public GetOngoingTaskInfoCommand(long taskId, OngoingTaskType type)
            {
                _taskId = taskId;
                _type = type;
            }

            public GetOngoingTaskInfoCommand(string taskName, OngoingTaskType type)
            {
                if (string.IsNullOrWhiteSpace(taskName))
                    throw new ArgumentException("Value cannot be null or whitespace.", nameof(taskName));
                _taskName = taskName;
                _type = type;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = _taskName != null ?
                    $"{node.Url}/databases/{node.Database}/task?taskName={Uri.EscapeDataString(_taskName)}&type={_type}" :
                    $"{node.Url}/databases/{node.Database}/task?key={_taskId}&type={_type}";

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
                        case OngoingTaskType.OlapEtl:
                            Result = JsonDeserializationClient.GetOngoingTaskOlapEtlResult(response);
                            break;
                        case OngoingTaskType.ElasticSearchEtl:
                            Result = JsonDeserializationClient.GetOngoingTaskElasticSearchEtlResult(response);
                            break;
                        case OngoingTaskType.QueueEtl:
                            Result = JsonDeserializationClient.GetOngoingTaskQueueEtlResult(response);
                            break;
                        case OngoingTaskType.SnowflakeEtl:
                            Result = JsonDeserializationClient.GetOngoingTaskSnowflakeEtlResult(response);
                            break;
                        case OngoingTaskType.Backup:
                            Result = JsonDeserializationClient.GetOngoingTaskBackupResult(response);
                            break;
                        case OngoingTaskType.Subscription:
                            Result = JsonDeserializationClient.GetOngoingTaskSubscriptionResult(response);
                            break;
                        case OngoingTaskType.PullReplicationAsSink:
                            Result = JsonDeserializationClient.OngoingTaskPullReplicationAsSinkResult(response);
                            break;
                        case OngoingTaskType.PullReplicationAsHub:
                            Result = JsonDeserializationClient.OngoingTaskPullReplicationAsHubResult(response);
                            break;
                        case OngoingTaskType.QueueSink:
                            Result = JsonDeserializationClient.GetOngoingTaskQueueSinkResult(response);
                            break;
                        default:
                            throw new ArgumentOutOfRangeException(nameof(_type), _type, "Unknown task type");
                    }
                }
            }

            public override bool IsReadRequest => false;
        }
    }
}
