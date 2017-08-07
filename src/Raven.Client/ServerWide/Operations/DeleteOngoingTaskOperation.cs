using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json.Converters;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Operations
{
    public class DeleteOngoingTaskOperation : IServerOperation<ModifyOngoingTaskResult>
    {
        private readonly string _database;
        private readonly long _taskId;
        private readonly OngoingTaskType _taskType;

        public DeleteOngoingTaskOperation(string database, long taskId, OngoingTaskType taskType)
        {
            Helpers.AssertValidDatabaseName(database);
            _database = database;
            _taskId = taskId;
            _taskType = taskType;
        }

        public RavenCommand<ModifyOngoingTaskResult> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new DeleteOngoingTaskCommand(_database, _taskId, _taskType);
        }

        private class DeleteOngoingTaskCommand : RavenCommand<ModifyOngoingTaskResult>
        {
            private readonly string _databaseName;
            private readonly long _taskId;
            private readonly OngoingTaskType _taskType;

            public DeleteOngoingTaskCommand(
                string database,
                long taskId,
                OngoingTaskType taskType

            )
            {
                _databaseName = database ?? throw new ArgumentNullException(nameof(database));
                _taskId = taskId;
                _taskType = taskType;
            }

            public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{_databaseName}/admin/tasks?id={_taskId}&type={_taskType}";

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Delete
                };

                return request;
            }

            public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    ThrowInvalidResponse();

                Result = JsonDeserializationClient.ModifyOngoingTaskResult(response);
            }

            public override bool IsReadRequest => false;
        }
    }

}
