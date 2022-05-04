using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client;
using Raven.Client.Exceptions.Documents.Indexes;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.Http;

namespace Raven.Server.Documents.Handlers.Processors.Indexes;

internal class IndexHandlerProcessorForReplace : AbstractIndexHandlerProcessorForReplace<DatabaseRequestHandler, DocumentsOperationContext>
{
    public IndexHandlerProcessorForReplace([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override bool SupportsCurrentNode => true;

    protected override ValueTask HandleCurrentNodeAsync()
    {
        var name = GetIndexName();
        var replacementName = Constants.Documents.Indexing.SideBySideIndexNamePrefix + name;

        var oldIndex = RequestHandler.Database.IndexStore.GetIndex(name);
        var newIndex = RequestHandler.Database.IndexStore.GetIndex(replacementName);

        if (oldIndex == null && newIndex == null)
            throw new IndexDoesNotExistException($"Could not find '{name}' and '{replacementName}' indexes.");

        if (newIndex == null)
            throw new IndexDoesNotExistException($"Could not find side-by-side index for '{name}'.");

        using (var token = RequestHandler.CreateOperationToken(TimeSpan.FromMinutes(15)))
        {
            RequestHandler.Database.IndexStore.ReplaceIndexes(name, newIndex.Name, token.Token);
        }

        return ValueTask.CompletedTask;
    }

    protected override Task HandleRemoteNodeAsync(ProxyCommand<object> command, OperationCancelToken token)
    {
        return RequestHandler.ExecuteRemoteAsync(command, token.Token);
    }
}
