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
    public class UpdateDatabaseTopology : IServerOperation<UpdateTopologyResult>
    {
        private readonly Dictionary<string, string> _updateBlockedConnections;
        private readonly List<DatabaseWatcher> _newWatchers;
        private readonly string _database;

        public UpdateDatabaseTopology(string database, Dictionary<string,string> updateUpdateBlockedConnections, List<DatabaseWatcher> newWatchers = null)
        {
            MultiDatabase.AssertValidName(database);
            _database = database;
            _updateBlockedConnections = updateUpdateBlockedConnections;
            _newWatchers = newWatchers;
        }

        public RavenCommand<UpdateTopologyResult> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new UpdateTopologyCommand(conventions, context, _database, _updateBlockedConnections, _newWatchers);
        }

        private class UpdateTopologyCommand : RavenCommand<UpdateTopologyResult>
        {
            private readonly JsonOperationContext _context;
            private readonly DocumentConventions _conventions;
            private readonly string _databaseName;
            private readonly Dictionary<string, string> _blockedConnections;
            private readonly List<DatabaseWatcher> _newWatchers;
          
            public UpdateTopologyCommand(
                DocumentConventions conventions, 
                JsonOperationContext context, 
                string database,
                Dictionary<string, string> blockedConnections,
                List<DatabaseWatcher> newWatchers
               
                )
            {
                _context = context ?? throw new ArgumentNullException(nameof(context));
                _conventions = conventions ?? throw new ArgumentNullException(nameof(conventions));
                _databaseName = database ?? throw new ArgumentNullException(nameof(database));
                _blockedConnections = blockedConnections;
                _newWatchers = newWatchers;
            }

            public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
            {
                url = $"{node.Url}/admin/update-topology?name={_databaseName}";
                
                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Post,
                    Content = new BlittableJsonContent(stream =>
                    {
                        var json = new DynamicJsonValue
                        {
                            ["BlockedConnections"] = DynamicJsonValue.Convert(_blockedConnections),
                            ["NewWatchers"] = new DynamicJsonArray(_newWatchers?.Select( w=> w.ToJson())),
                        };

                        _context.Write(stream, _context.ReadObject(json, "updated-topology"));
;
                    })
                };

                return request;
            }

            public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    ThrowInvalidResponse();

                Result = JsonDeserializationClient.UpdateTopologyResult(response);
            }

            public override bool IsReadRequest => false;
        }
    }
}
