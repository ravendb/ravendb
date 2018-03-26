using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Raven.Client.Exceptions.Documents.Indexes;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Debugging
{
    public class QueriesDebugHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/debug/queries/kill", "POST", AuthorizationStatus.ValidUser)]
        public Task KillQuery()
        {
            var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("indexName");
            var id = GetLongQueryString("id");

            var index = Database.IndexStore.GetIndex(name);
            if (index == null)
                IndexDoesNotExistException.ThrowFor(name);

            var query = index.CurrentlyRunningQueries
                .FirstOrDefault(q => q.QueryId == id);

            if (query == null)
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                return Task.CompletedTask;
            }

            query.Token.Cancel();

            return NoContent();
        }

        [RavenAction("/databases/*/debug/queries/running", "GET", AuthorizationStatus.ValidUser, IsDebugInformationEndpoint = true)]
        public Task RunningQueries()
        {
            var indexes = Database
                .IndexStore
                .GetIndexes()
                .ToList();

            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                writer.WriteStartObject();
                var isFirst = true;
                foreach (var index in indexes)
                {
                    if (isFirst == false)
                        writer.WriteComma();
                    isFirst = false;

                    writer.WritePropertyName(index.Name);
                    writer.WriteStartArray();

                    var isFirstInternal = true;
                    foreach (var query in index.CurrentlyRunningQueries)
                    {
                        if (isFirstInternal == false)
                            writer.WriteComma();

                        isFirstInternal = false;

                        writer.WriteStartObject();

                        writer.WritePropertyName((nameof(query.Duration)));
                        writer.WriteString(query.Duration.ToString());
                        writer.WriteComma();

                        writer.WritePropertyName((nameof(query.QueryId)));
                        writer.WriteInteger(query.QueryId);
                        writer.WriteComma();

                        writer.WritePropertyName((nameof(query.StartTime)));
                        writer.WriteDateTime(query.StartTime, isUtc: true);
                        writer.WriteComma();

                        writer.WritePropertyName((nameof(query.QueryInfo)));
                        writer.WriteIndexQuery(context, query.QueryInfo);

                        writer.WriteEndObject();
                    }

                    writer.WriteEndArray();
                }

                writer.WriteEndObject();
            }

            return Task.CompletedTask;
        }
    }
}
