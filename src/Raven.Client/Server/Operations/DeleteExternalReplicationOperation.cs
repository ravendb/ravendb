using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json.Converters;
using Sparrow.Json;

namespace Raven.Client.Server.Operations
{
    public class DeleteExternalReplicationOperation : IServerOperation<ModifyExternalReplicationResult>
    {
        private readonly string _database;
        private readonly long _taskId;

        public DeleteExternalReplicationOperation(string database, long taskId)
        {
            MultiDatabase.AssertValidName(database);
            _database = database;
            _taskId = taskId;
        }

        public RavenCommand<ModifyExternalReplicationResult> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new DeleteExternalReplicationCommand(conventions, context, _database, _taskId);
        }

        private class DeleteExternalReplicationCommand : RavenCommand<ModifyExternalReplicationResult>
        {
            private readonly JsonOperationContext _context;
            private readonly DocumentConventions _conventions;
            private readonly string _databaseName;
            private readonly long _taskId;

            public DeleteExternalReplicationCommand(
                DocumentConventions conventions,
                JsonOperationContext context,
                string database,
                long taskId

            )
            {
                _context = context ?? throw new ArgumentNullException(nameof(context));
                _conventions = conventions ?? throw new ArgumentNullException(nameof(conventions));
                _databaseName = database ?? throw new ArgumentNullException(nameof(database));
                _taskId = taskId;
            }

            public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
            {
                url = $"{node.Url}/admin/external-replication/delete?name={_databaseName}&id={_taskId}";

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Post
                };

                return request;
            }

            public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    ThrowInvalidResponse();

                Result = JsonDeserializationClient.ModifyExternalReplicationResult(response);
            }

            public override bool IsReadRequest => false;
        }
    }

}
