using System;
using System.IO;
using System.Net;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Indexes;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.Http;

namespace Raven.Server.Documents.Handlers.Processors.Indexes;

internal class IndexProcessorForGenerateCSharpIndexDefinition : AbstractIndexProcessorForGenerateCSharpIndexDefinition<DatabaseRequestHandler, DocumentsOperationContext>
{
    public IndexProcessorForGenerateCSharpIndexDefinition([NotNull] DatabaseRequestHandler requestHandler)
        : base(requestHandler, requestHandler.ContextPool)
    {
    }

    protected override bool SupportsCurrentNode => true;

    protected override ValueTask HandleCurrentNodeAsync()
    {
        var name = GetName();
        var index = RequestHandler.Database.IndexStore.GetIndex(name);
        if (index == null)
        {
            HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
            return ValueTask.CompletedTask;
        }

        if (index.Type.IsAuto())
            throw new InvalidOperationException("Can't create C# index definition from auto indexes");

        var indexDefinition = index.GetIndexDefinition();

        return WriteResultAsync(new IndexDefinitionCodeGenerator(indexDefinition).Generate());
    }

    protected override Task HandleRemoteNodeAsync(ProxyCommand<string> command, OperationCancelToken token) => RequestHandler.ExecuteRemoteAsync(command, token.Token);

    private async ValueTask WriteResultAsync(string result)
    {
        await using (var writer = new StreamWriter(RequestHandler.ResponseBodyStream()))
        {
            await writer.WriteAsync(result);
        }
    }
}
