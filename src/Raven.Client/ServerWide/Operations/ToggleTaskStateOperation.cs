using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json.Converters;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Operations
{
    public class ToggleTaskStateOperation : IServerOperation
    {
        private readonly string _database;
        private readonly long _taskId;
        private readonly OngoingTaskType _type;
        private readonly bool _disable;

        public ToggleTaskStateOperation(string database, long taskId, OngoingTaskType type, bool disable)
        {
            Helpers.AssertValidDatabaseName(database);
            _database = database;
            _taskId = taskId;
            _type = type;
            _disable = disable;
        }

        public RavenCommand GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new ToggleTaskStateCommand(_database, _taskId, _type, _disable);
        }

        private class ToggleTaskStateCommand : RavenCommand
        {
            private readonly string _databaseName;
            private readonly long _taskId;
            private readonly OngoingTaskType _type;
            private readonly bool _disable;

            public ToggleTaskStateCommand(
                string database,
                long taskId,
                OngoingTaskType type,
                bool disable)
            {
                _databaseName = database ?? throw new ArgumentNullException(nameof(database));
                _taskId = taskId;
                _type = type;
                _disable = disable;
            }

            public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{_databaseName}/admin/tasks/state?key={_taskId}&type={_type}&disable={_disable}";

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Post
                };

                return request;
            }

            public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
            {
                if (response != null)
                    Result = JsonDeserializationClient.ModifyOngoingTaskResult(response);
            }

            public override bool IsReadRequest => false;
        }
    }

}
