using System.Linq;
using System.Net;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.Documents.Queries;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Debugging.Processors;

internal abstract class AbstractQueriesDebugHandlerProcessorForKillQuery<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    protected AbstractQueriesDebugHandlerProcessorForKillQuery([NotNull] TRequestHandler requestHandler)
        : base(requestHandler)
    {
    }

    protected abstract AbstractQueryRunner GetQueryRunner();

    public override ValueTask ExecuteAsync()
    {
        string clientQueryId = RequestHandler.GetStringQueryString("clientQueryId", required: false);
        ExecutingQueryInfo query;

        var queryRunner = GetQueryRunner();

        if (clientQueryId != null)
        {
            query = queryRunner.CurrentlyRunningQueries
                .FirstOrDefault(x => x.QueryInfo is IndexQueryServerSide q && q.ClientQueryId == clientQueryId);
        }
        else
        {
            var name = RequestHandler.GetQueryStringValueAndAssertIfSingleAndNotEmpty("indexName");
            var id = RequestHandler.GetLongQueryString("id");

            query = queryRunner.CurrentlyRunningQueries
                .FirstOrDefault(x => x.IndexName == name && x.QueryId == id);
        }

        if (query == null)
        {
            HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
            return ValueTask.CompletedTask;
        }

        query.Token.Cancel();

        RequestHandler.NoContentStatus();
        return ValueTask.CompletedTask;
    }
}
