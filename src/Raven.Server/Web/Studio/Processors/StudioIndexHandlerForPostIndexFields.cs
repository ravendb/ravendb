using JetBrains.Annotations;
using Raven.Server.Config;
using Raven.Server.Documents;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Web.Studio.Processors;

internal class StudioIndexHandlerForPostIndexFields : AbstractStudioIndexHandlerForPostIndexFields<DatabaseRequestHandler, DocumentsOperationContext>
{
    public StudioIndexHandlerForPostIndexFields([NotNull] DatabaseRequestHandler requestHandler) 
        : base(requestHandler, requestHandler.ContextPool)
    {
    }

    protected override RavenConfiguration GetDatabaseConfiguration() => RequestHandler.Database.Configuration;
}
