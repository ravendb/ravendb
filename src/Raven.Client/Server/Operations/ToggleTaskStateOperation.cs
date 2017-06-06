using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json.Converters;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Server.Operations
{
    public class ToggleTaskStateOperation : IServerOperation
    {
        private readonly string _database;
        private readonly long _taskId;
        private readonly OngoingTaskType _type;
        private readonly bool _disable;

        public ToggleTaskStateOperation(string database, long taskId, OngoingTaskType type, bool disable)
        {
            MultiDatabase.AssertValidName(database);
            _database = database;
            _taskId = taskId;
            _type = type;
            _disable = disable;
        }

        public RavenCommand GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new ToggleTaskStateCommand(conventions, context, _database, _taskId, _type, _disable);
        }

        private class ToggleTaskStateCommand : RavenCommand
        {
            private readonly JsonOperationContext _context;
            private readonly DocumentConventions _conventions;
            private readonly string _databaseName;
            private readonly long _taskId;
            private readonly OngoingTaskType _type;
            private readonly bool _disable;

            public ToggleTaskStateCommand(
                DocumentConventions conventions,
                JsonOperationContext context,
                string database,
                long taskId,
                OngoingTaskType type,
                bool disable
            )
            {
                _context = context ?? throw new ArgumentNullException(nameof(context));
                _conventions = conventions ?? throw new ArgumentNullException(nameof(conventions));
                _databaseName = database ?? throw new ArgumentNullException(nameof(database));
                _taskId = taskId;
                _type = type;
                _disable = disable;
            }

            public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
            {
                url = $"{node.Url}/admin/tasks/status?name={_databaseName}&key={_taskId}&type={_type}&disable={_disable}";

                var request = new HttpRequestMessage
                {
                    Method = HttpMethods.Patch
                };

                return request;
            }

            public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
            {
                if (response != null)
                    Result = JsonDeserializationClient.ModifyExternalReplicationResult(response);
            }

            public override bool IsReadRequest => false;
        }
    }

}
