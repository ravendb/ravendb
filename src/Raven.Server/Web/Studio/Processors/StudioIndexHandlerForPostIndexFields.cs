using System.Threading;
using JetBrains.Annotations;
using Raven.Server.Config;
using Raven.Server.Documents;
using Raven.Server.ServerWide.Context;
using NotImplementedException = System.NotImplementedException;

namespace Raven.Server.Web.Studio.Processors;

internal class StudioIndexHandlerForPostIndexFields : AbstractStudioIndexHandlerForPostIndexFields<DatabaseRequestHandler, DocumentsOperationContext>
{
    public StudioIndexHandlerForPostIndexFields([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
        DatabaseShutdown = RequestHandler.Database.DatabaseShutdown;
    }

    protected override RavenConfiguration GetDatabaseConfiguration() => RequestHandler.Database.Configuration;
}
