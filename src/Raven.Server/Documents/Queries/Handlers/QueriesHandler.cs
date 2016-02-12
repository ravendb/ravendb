using System.Threading.Tasks;

using Raven.Server.Routing;

namespace Raven.Server.Documents.Queries.Handlers
{
    public class QueriesHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/queries", "GET")]
        public Task Get()
        {
            var indexName = GetStringQueryString("name");
            var query = GetIndexQuery(Database.DocumentsStorage.Configuration.Core.MaxPageSize);

            var runner = new QueryRunner(IndexStore);

            var result = runner.ExecuteQuery(indexName, query);

            return Task.CompletedTask;
        }
    }
}