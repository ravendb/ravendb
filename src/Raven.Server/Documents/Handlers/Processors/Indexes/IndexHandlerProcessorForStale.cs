using System.Collections.Generic;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Exceptions.Documents.Indexes;
using Raven.Server.Documents.Commands.Indexes;
using Raven.Server.Documents.Indexes;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.Http;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Indexes;

internal class IndexHandlerProcessorForStale : AbstractIndexHandlerProcessorForStale<DatabaseRequestHandler, DocumentsOperationContext>
{
    public IndexHandlerProcessorForStale([NotNull] DatabaseRequestHandler requestHandler)
        : base(requestHandler, requestHandler.ContextPool)
    {
    }

    protected override bool SupportsCurrentNode => true;

    protected override ValueTask HandleCurrentNodeAsync()
    {
        var name = GetName();

        var index = RequestHandler.Database.IndexStore.GetIndex(name);
        if (index == null)
            IndexDoesNotExistException.ThrowFor(name);

        using (var context = QueryOperationContext.Allocate(RequestHandler.Database, index))
        using (context.OpenReadTransaction())
        {
            var stalenessReasons = new List<string>();
            var isStale = index.IsStale(context, stalenessReasons: stalenessReasons);

            return WriteResultAsync(new GetIndexStalenessCommand.IndexStaleness
            {
                IsStale = isStale,
                StalenessReasons = stalenessReasons
            });
        }
    }

    protected override Task HandleRemoteNodeAsync(ProxyCommand<GetIndexStalenessCommand.IndexStaleness> command) => RequestHandler.ExecuteRemoteAsync(command);

    private async ValueTask WriteResultAsync(GetIndexStalenessCommand.IndexStaleness result)
    {
        using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
        await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
        {
            writer.WriteStartObject();

            writer.WritePropertyName(nameof(result.IsStale));
            writer.WriteBool(result.IsStale);
            writer.WriteComma();

            writer.WriteArray(nameof(result.StalenessReasons), result.StalenessReasons);

            writer.WriteEndObject();
        }
    }
}
