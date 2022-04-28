using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Indexes;
using Raven.Client.Exceptions.Documents.Indexes;
using Raven.Server.Documents.Indexes;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.Http;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Indexes;

internal class IndexHandlerProcessorForGetErrors : AbstractIndexHandlerProcessorForGetErrors<DatabaseRequestHandler, DocumentsOperationContext>
{
    public IndexHandlerProcessorForGetErrors([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override bool SupportsCurrentNode => true;

    protected override ValueTask HandleCurrentNodeAsync()
    {
        var names = GetIndexNames();

        List<Index> indexes;
        if (names == null || names.Length == 0)
            indexes = RequestHandler.Database.IndexStore.GetIndexes().ToList();
        else
        {
            indexes = new List<Index>();
            foreach (var name in names)
            {
                var index = RequestHandler.Database.IndexStore.GetIndex(name);
                if (index == null)
                    IndexDoesNotExistException.ThrowFor(name);

                indexes.Add(index);
            }
        }

        var indexErrors = indexes.Select(x => new IndexErrors
        {
            Name = x.Name,
            Errors = x.GetErrors().ToArray()
        }).ToArray();

        return WriteResultAsync(indexErrors);
    }

    protected override Task HandleRemoteNodeAsync(ProxyCommand<IndexErrors[]> command) => RequestHandler.ExecuteRemoteAsync(command);

    private async ValueTask WriteResultAsync(IndexErrors[] result)
    {
        using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
        await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
            writer.WriteIndexErrors(context, result);
    }
}
