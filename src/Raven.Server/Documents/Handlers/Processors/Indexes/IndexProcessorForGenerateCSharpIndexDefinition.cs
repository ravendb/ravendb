using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Indexes;
using Raven.Client.Http;
using Raven.Server.Documents.Indexes;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.Indexes;

internal class IndexProcessorForGenerateCSharpIndexDefinition : AbstractIndexProcessorForGenerateCSharpIndexDefinition<DatabaseRequestHandler, DocumentsOperationContext>
{
    public IndexProcessorForGenerateCSharpIndexDefinition([NotNull] DatabaseRequestHandler requestHandler)
        : base(requestHandler, requestHandler.ContextPool)
    {
    }

    protected override bool SupportsCurrentNode => true;

    protected override ValueTask<string> GetResultForCurrentNodeAsync()
    {
        var name = GetName();
        var index = RequestHandler.Database.IndexStore.GetIndex(name);
        if (index == null)
            return ValueTask.FromResult<string>(null);

        if (index.Type.IsAuto())
            throw new InvalidOperationException("Can't create C# index definition from auto indexes");

        var indexDefinition = index.GetIndexDefinition();

        return ValueTask.FromResult(new IndexDefinitionCodeGenerator(indexDefinition).Generate());
    }

    protected override Task<string> GetResultForRemoteNodeAsync(RavenCommand<string> command) => RequestHandler.ExecuteRemoteAsync(command);
}
