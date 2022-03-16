using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Indexes;
using Raven.Client.Exceptions.Documents.Indexes;
using Raven.Client.Http;
using Raven.Server.Documents.Indexes;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors;

internal class IndexHandlerProcessorForGetErrors : AbstractIndexHandlerProcessorForGetErrors<DatabaseRequestHandler, DocumentsOperationContext>
{
    public IndexHandlerProcessorForGetErrors([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler, requestHandler.ContextPool)
    {
    }

    protected override bool SupportsCurrentNode => true;

    protected override ValueTask<IndexErrors[]> GetResultForCurrentNodeAsync()
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

        return ValueTask.FromResult(indexErrors);
    }

    protected override Task<IndexErrors[]> GetResultForRemoteNodeAsync(RavenCommand<IndexErrors[]> command) => RequestHandler.ExecuteRemoteAsync(command);
}
