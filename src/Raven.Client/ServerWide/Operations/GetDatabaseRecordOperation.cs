using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Operations
{
    public class GetDatabaseRecordOperation : IServerOperation<DatabaseRecordWithEtag>
    {
        private readonly string _database;

        public GetDatabaseRecordOperation(string database)
        {
            _database = database;
        }

        public RavenCommand<DatabaseRecordWithEtag> GetCommand(DocumentConventions conventions, JsonOperationContext ctx)
        {
            return new GetDatabaseRecordCommand(_database);
        }

        private class GetDatabaseRecordCommand : RavenCommand<DatabaseRecordWithEtag>
        {
            private readonly string _database;

            public override bool IsReadRequest => false;

            public GetDatabaseRecordCommand(string database)
            {
                _database = database;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/admin/databases?name={_database}";
                return new HttpRequestMessage
                {
                    Method = HttpMethod.Get
                };
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                {
                    Result = null;
                    return;
                }

                Result = DocumentConventions.Default.Serialization.DefaultConverter.FromBlittable<DatabaseRecordWithEtag>(response, "database/record");
            }
        }
    }
}
