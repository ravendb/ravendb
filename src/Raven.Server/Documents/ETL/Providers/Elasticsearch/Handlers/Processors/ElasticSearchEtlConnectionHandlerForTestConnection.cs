using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Processors;
using Sparrow.Json;

namespace Raven.Server.Documents.ETL.Providers.ElasticSearch.Handlers.Processors;

internal sealed class ElasticSearchEtlConnectionHandlerForTestConnection<TOperationContext> : AbstractDatabaseHandlerProcessor<TOperationContext>
    where TOperationContext : JsonOperationContext
{
    public ElasticSearchEtlConnectionHandlerForTestConnection([NotNull] AbstractDatabaseRequestHandler<TOperationContext> requestHandler)
        : base(requestHandler)
    {
    }

    public override ValueTask ExecuteAsync() => new(ElasticSearchEtlTestConnectionHelper.ExecuteAsync(RequestHandler));
}
