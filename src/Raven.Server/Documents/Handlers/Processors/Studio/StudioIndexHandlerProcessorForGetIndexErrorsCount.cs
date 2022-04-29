using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Exceptions.Documents.Indexes;
using Raven.Server.Documents.Commands.Indexes;
using Raven.Server.Documents.Indexes;
using Raven.Server.Json;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.Http;
using Raven.Server.Web.Studio.Processors;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Studio;

internal class StudioIndexHandlerProcessorForGetIndexErrorsCount : AbstractStudioIndexHandlerProcessorForGetIndexErrorsCount<DatabaseRequestHandler, DocumentsOperationContext>
{
    public StudioIndexHandlerProcessorForGetIndexErrorsCount([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler, requestHandler.ContextPool)
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

        var indexErrorsCounts = indexes.Select(x => new GetIndexErrorsCountCommand.IndexErrorsCount
        {
            Name = x.Name,
            Errors = x.GetErrors()
                .GroupBy(y => y.Action)
                .Select(y => new GetIndexErrorsCountCommand.IndexingErrorCount
                {
                    Action = y.Key,
                    NumberOfErrors = y.Count()
                }).ToArray()
        }).ToArray();

        return WriteResultAsync(indexErrorsCounts);
    }

    protected override Task HandleRemoteNodeAsync(ProxyCommand<GetIndexErrorsCountCommand.IndexErrorsCount[]> command, OperationCancelToken token) => RequestHandler.ExecuteRemoteAsync(command, token.Token);

    private async ValueTask WriteResultAsync(GetIndexErrorsCountCommand.IndexErrorsCount[] result)
    {
        using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
        await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
            writer.WriteIndexErrorCounts(context, result);
    }
}
