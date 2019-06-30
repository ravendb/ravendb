using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Text;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Session;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Operations
{
    public class UpdateUnusedDatabasesOperation : IServerOperation
    {
        private readonly string _database;
        private readonly HashSet<string> _unusedDatabaseIds;

        public UpdateUnusedDatabasesOperation(string database, HashSet<string> unusedDatabaseIds)
        {
            if (string.IsNullOrEmpty(database))
                throw new ArgumentException(database);

            _database = database;
            _unusedDatabaseIds = unusedDatabaseIds;
        }

        public RavenCommand GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {

            return new UpdateUnusedDatabasesCommand(_database, _unusedDatabaseIds);
        }

        private class UpdateUnusedDatabasesCommand : RavenCommand, IRaftCommand
        {
            private readonly string _database;
            private readonly Parameters _parameters;

            public UpdateUnusedDatabasesCommand(string database, HashSet<string> unusedDatabaseIds)
            {
                _database = database;
                _parameters = new Parameters
                {
                    DatabaseIds = unusedDatabaseIds
                };
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/admin/databases/unused-ids?name={_database}";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Post,
                    Content = new BlittableJsonContent(stream =>
                    {
                        ctx.Write(stream, EntityToBlittable.ConvertCommandToBlittable(_parameters, ctx));
                    })
                };
            }

            public string RaftUniqueRequestId => RaftIdGenerator.NewId();
        }
        public class Parameters
        {
            public HashSet<string> DatabaseIds { get; set; }
        }
    }
}
