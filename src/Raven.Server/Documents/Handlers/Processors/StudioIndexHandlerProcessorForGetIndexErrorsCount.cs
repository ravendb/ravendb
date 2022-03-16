using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Exceptions.Documents.Indexes;
using Raven.Client.Http;
using Raven.Server.Documents.Commands;
using Raven.Server.Documents.Indexes;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.Studio.Processors;

namespace Raven.Server.Documents.Handlers.Processors;

internal class StudioIndexHandlerProcessorForGetIndexErrorsCount : AbstractStudioIndexHandlerProcessorForGetIndexErrorsCount<DatabaseRequestHandler, DocumentsOperationContext>
{
    public StudioIndexHandlerProcessorForGetIndexErrorsCount([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler, requestHandler.ContextPool)
    {
    }

    protected override bool SupportsCurrentNode => true;

    protected override ValueTask<GetIndexErrorsCountCommand.IndexErrorsCount[]> GetResultForCurrentNodeAsync()
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

        var indexErrorsCounts = indexes.Select(x => new GetIndexErrorsCountCommand.IndexErrorsCount
        {
            Name = x.Name,
            NumberOfErrors = x.GetErrorCount()
        }).ToArray();

        return ValueTask.FromResult(indexErrorsCounts);
    }

    protected override Task<GetIndexErrorsCountCommand.IndexErrorsCount[]> GetResultForRemoteNodeAsync(RavenCommand<GetIndexErrorsCountCommand.IndexErrorsCount[]> command) => RequestHandler.ExecuteRemoteAsync(command);
}
