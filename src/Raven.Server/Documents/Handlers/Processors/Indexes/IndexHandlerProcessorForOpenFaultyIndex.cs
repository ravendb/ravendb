using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Exceptions.Documents.Indexes;
using Raven.Client.Http;
using Raven.Server.Documents.Indexes.Errors;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.Http;

namespace Raven.Server.Documents.Handlers.Processors.Indexes;

internal class IndexHandlerProcessorForOpenFaultyIndex : AbstractIndexHandlerProcessorForOpenFaultyIndex<DatabaseRequestHandler, DocumentsOperationContext>
{
    public IndexHandlerProcessorForOpenFaultyIndex([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override bool SupportsCurrentNode => true;

    protected override ValueTask HandleCurrentNodeAsync()
    {
        var name = GetName();

        var index = RequestHandler.Database.IndexStore.GetIndex(name);
        if (index == null)
            IndexDoesNotExistException.ThrowFor(name);

        if (index is FaultyInMemoryIndex == false)
            throw new InvalidOperationException($"Cannot open non faulty index named: {name}");

        lock (index)
        {
            var localIndex = RequestHandler.Database.IndexStore.GetIndex(name);
            if (localIndex == null)
                IndexDoesNotExistException.ThrowFor(name);

            if (localIndex is FaultyInMemoryIndex == false)
                throw new InvalidOperationException($"Cannot open non faulty index named: {name}");

            RequestHandler.Database.IndexStore.OpenFaultyIndex(localIndex);
        }

        return ValueTask.CompletedTask;
    }

    protected override Task HandleRemoteNodeAsync(ProxyCommand<object> command, OperationCancelToken token) => RequestHandler.ExecuteRemoteAsync(command, token.Token);
}
