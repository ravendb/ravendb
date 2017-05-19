using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.Server.Operations
{
    public class GetDatabaseTopologyOperation : IServerOperation<DatabaseTopology>
    {
        private readonly string _database;

        public GetDatabaseTopologyOperation(string database)
        {
            _database = database;
        }

        public RavenCommand<DatabaseTopology> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new GetDatabaseTopologyCommand(_database);
        }
    }


    public class GetDatabaseTopologyCommand : RavenCommand<DatabaseTopology>
    {
        private readonly string _database;
        private readonly DocumentConventions _conventions = new DocumentConventions();

        public override bool IsReadRequest => false;

        public GetDatabaseTopologyCommand(string database)
        {
            _database = database;
        }

        public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
        {
            url = $"{node.Url}/admin/databases?name={_database}";
            return new HttpRequestMessage
            {
                Method = HttpMethod.Get,
            };
        }

        public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
        {
            if (response == null)
            {
                Result = null;
                return;
            }

            var rec = (DatabaseRecord)EntityToBlittable.ConvertToEntity(typeof(DatabaseRecord), "database-record", response, _conventions);
            Result = rec.Topology;
        }
    }
}
