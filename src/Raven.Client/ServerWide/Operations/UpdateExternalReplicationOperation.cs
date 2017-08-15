using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Converters;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.ServerWide.Operations
{
    public class UpdateExternalReplicationOperation : IServerOperation<ModifyOngoingTaskResult>
    {
        private readonly ExternalReplication _newWatcher;
        private readonly string _database;

        public UpdateExternalReplicationOperation(string database, ExternalReplication newWatcher)
        {
            Helpers.AssertValidDatabaseName(database);
            _database = database;
            _newWatcher = newWatcher;
        }

        public RavenCommand<ModifyOngoingTaskResult> GetCommand(DocumentConventions conventions, JsonOperationContext ctx)
        {
            return new UpdateExternalReplication(_database, _newWatcher);
        }

        private class UpdateExternalReplication : RavenCommand<ModifyOngoingTaskResult>
        {
            private readonly string _databaseName;
            private readonly ExternalReplication _newWatcher;

            public UpdateExternalReplication(string database, ExternalReplication newWatcher)
            {
                _databaseName = database ?? throw new ArgumentNullException(nameof(database));
                _newWatcher = newWatcher;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{_databaseName}/admin/tasks/external-replication";

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Post,
                    Content = new BlittableJsonContent(stream =>
                    {
                        var json = new DynamicJsonValue
                        {
                            ["Watcher"] = _newWatcher.ToJson()
                        };

                        ctx.Write(stream, ctx.ReadObject(json, "update-replication"));
                    })
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
