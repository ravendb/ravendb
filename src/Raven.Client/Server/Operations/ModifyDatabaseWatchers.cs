using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Converters;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Server.Operations
{
    public class ModifyDatabaseWatchers : IServerOperation<ModifyDatabaseWatchersResult>
    {
        private readonly List<DatabaseWatcher> _newWatchers;
        private readonly string _database;

        public ModifyDatabaseWatchers(string database, List<DatabaseWatcher> newWatchers = null)
        {
            MultiDatabase.AssertValidName(database);
            _database = database;
            _newWatchers = newWatchers;
        }

        public RavenCommand<ModifyDatabaseWatchersResult> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new ModifyDatabaseWatchersCommand(conventions, context, _database, _newWatchers);
        }

        private class ModifyDatabaseWatchersCommand : RavenCommand<ModifyDatabaseWatchersResult>
        {
            private readonly JsonOperationContext _context;
            private readonly DocumentConventions _conventions;
            private readonly string _databaseName;
            private readonly List<DatabaseWatcher> _newWatchers;
          
            public ModifyDatabaseWatchersCommand(
                DocumentConventions conventions, 
                JsonOperationContext context, 
                string database,
                List<DatabaseWatcher> newWatchers
               
                )
            {
                _context = context ?? throw new ArgumentNullException(nameof(context));
                _conventions = conventions ?? throw new ArgumentNullException(nameof(conventions));
                _databaseName = database ?? throw new ArgumentNullException(nameof(database));
                _newWatchers = newWatchers;
            }

            public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
            {
                url = $"{node.Url}/admin/modify-watchers?name={_databaseName}";
                
                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Post,
                    Content = new BlittableJsonContent(stream =>
                    {
                        var json = new DynamicJsonValue
                        {
                            [nameof(DatabaseTopology.Watchers)] = new DynamicJsonArray(_newWatchers?.Select( w=> w.ToJson())),
                        };

                        _context.Write(stream, _context.ReadObject(json, "modify-watchers"));
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
