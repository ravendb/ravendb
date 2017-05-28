using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Converters;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Server.Operations
{
    public class UpdateWatcherOperation : IServerOperation<ModifyDatabaseWatchersResult>
    {
        private readonly DatabaseWatcher _newWatcher;
        private readonly string _database;

        public UpdateWatcherOperation(string database, DatabaseWatcher newWatcher)
        {
            MultiDatabase.AssertValidName(database);
            _database = database;
            _newWatcher = newWatcher;
        }

        public RavenCommand<ModifyDatabaseWatchersResult> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new UpdateWatcherCommand(conventions, context, _database, _newWatcher);
        }

        private class UpdateWatcherCommand : RavenCommand<ModifyDatabaseWatchersResult>
        {
            private readonly JsonOperationContext _context;
            private readonly DocumentConventions _conventions;
            private readonly string _databaseName;
            private readonly DatabaseWatcher _newWatcher;

            public UpdateWatcherCommand(
                DocumentConventions conventions,
                JsonOperationContext context,
                string database,
                DatabaseWatcher newWatcher

            )
            {
                _context = context ?? throw new ArgumentNullException(nameof(context));
                _conventions = conventions ?? throw new ArgumentNullException(nameof(conventions));
                _databaseName = database ?? throw new ArgumentNullException(nameof(database));
                _newWatcher = newWatcher;
            }

            public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
            {
                url = $"{node.Url}/admin/update-watcher?name={_databaseName}";

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Post,
                    Content = new BlittableJsonContent(stream =>
                    {
                        var json = new DynamicJsonValue
                        {
                            [nameof(DatabaseWatcher)] = _newWatcher.ToJson(),
                        };

                        _context.Write(stream, _context.ReadObject(json, "update-watcher"));
                    })
                };

                return request;
            }

            public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    ThrowInvalidResponse();

                Result = JsonDeserializationClient.ModifyDatabaseWatchersResult(response);
            }

            public override bool IsReadRequest => false;
        }
    }

}
