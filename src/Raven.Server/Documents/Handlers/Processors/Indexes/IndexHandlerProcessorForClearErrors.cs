using System.Collections.Generic;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Exceptions.Documents.Indexes;
using Raven.Client.Http;
using Raven.Server.Documents.Indexes;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Indexes;

internal class IndexHandlerProcessorForClearErrors : AbstractIndexHandlerProcessorForClearErrors<DatabaseRequestHandler, DocumentsOperationContext>
{
    public IndexHandlerProcessorForClearErrors([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override bool SupportsCurrentNode => true;

    protected override ValueTask ExecuteForCurrentNodeAsync()
    {
        var names = GetNames();

        var indexes = new List<Index>();

        if (names.Count == 0)
            indexes.AddRange(RequestHandler.Database.IndexStore.GetIndexes());
        else
        {
            foreach (var name in names)
            {
                var index = RequestHandler.Database.IndexStore.GetIndex(name);
                if (index == null)
                    IndexDoesNotExistException.ThrowFor(name);

                indexes.Add(index);
            }
        }

        foreach (var index in indexes)
            index.DeleteErrors();

        return ValueTask.CompletedTask;
    }

    protected override Task ExecuteForRemoteNodeAsync(RavenCommand command) => RequestHandler.ExecuteRemoteAsync(command);
}
