using JetBrains.Annotations;
using Raven.Server.Config;
using Raven.Server.Documents.Indexes;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Admin.Processors.Indexes;

internal class AdminIndexHandlerProcessorForJavaScriptPut : AbstractAdminIndexHandlerProcessorForJavaScriptPut<DatabaseRequestHandler, DocumentsOperationContext>
{
    public AdminIndexHandlerProcessorForJavaScriptPut([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override AbstractIndexCreateController GetIndexCreateProcessor() => RequestHandler.Database.IndexStore.Create;

    protected override RavenConfiguration GetDatabaseConfiguration() => RequestHandler.Database.Configuration;
}
