using System;
using System.Linq;
using System.Net;
using System.Threading.Tasks;

using Raven.Server.Routing;

namespace Raven.Server.Documents.Handlers.Admin
{
    public class QueriesAdminHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/debug/queries/kill", "GET")]
        public Task KillQuery()
        {
            var names = GetQueryStringValueAndAssertIfSingleAndNotEmpty("indexName");
            var ids = GetQueryStringValueAndAssertIfSingleAndNotEmpty("id");

            var idStr = ids[0];

            long id;
            if (long.TryParse(idStr, out id) == false)
                throw new ArgumentException($"Query string value 'id' must be a number");

            var index = Database.IndexStore.GetIndex(names[0]);
            if (index == null)
                throw new InvalidOperationException("There is not index with name: " + names[0]);

            var query = index.CurrentlyRunningQueries
                .FirstOrDefault(q => q.QueryId == id);

            if (query == null)
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                return Task.CompletedTask;
            }

            query.Token.Cancel();

            HttpContext.Response.StatusCode = (int)HttpStatusCode.NoContent;
            return Task.CompletedTask;
        }
    }
}