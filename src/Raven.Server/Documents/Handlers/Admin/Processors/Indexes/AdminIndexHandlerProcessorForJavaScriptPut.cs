using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Microsoft.AspNetCore.Http.Features.Authentication;
using Raven.Client.Documents.Smuggler;
using Raven.Server.Config;
using Raven.Server.Documents.Indexes;
using Raven.Server.ServerWide.Context;
using Raven.Server.Smuggler.Documents;
using Raven.Server.Smuggler.Documents.Data;
using Raven.Server.Smuggler.Migration;
using Sparrow.Json;
using static Raven.Server.RavenServer;

namespace Raven.Server.Documents.Handlers.Admin.Processors.Indexes;

internal sealed class AdminIndexHandlerProcessorForJavaScriptPut : AbstractAdminIndexHandlerProcessorForJavaScriptPut<DatabaseRequestHandler, DocumentsOperationContext>
{
    public AdminIndexHandlerProcessorForJavaScriptPut([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override AbstractIndexCreateController GetIndexCreateProcessor() => RequestHandler.Database.IndexStore.Create;

    protected override RavenConfiguration GetDatabaseConfiguration() => RequestHandler.Database.Configuration;

    protected override async ValueTask HandleIndexesFromLegacyReplicationAsync()
    {
        var authenticateConnection = HttpContext.Features.Get<IHttpAuthenticationFeature>() as AuthenticateConnection;
        if (authenticateConnection != null &&
            authenticateConnection.CanAccess(RequestHandler.DatabaseName, requireAdmin: true, requireWrite: true) == false)
        {
            throw new UnauthorizedAccessException("Deploying indexes from legacy replication requires admin privileges.");
        }

        var options = new DatabaseSmugglerOptionsServerSide(RequestHandler.GetAuthorizationStatusForSmuggler(RequestHandler.DatabaseName))
        {
            OperateOnTypes = DatabaseItemType.Indexes
        };
        using (ContextPool.AllocateOperationContext(out JsonOperationContext jsonOperationContext))
        await using (var stream = new ArrayStream(RequestHandler.RequestBodyStream(), nameof(DatabaseItemType.Indexes)))
        using (var source = new StreamSource(stream, jsonOperationContext, RequestHandler.DatabaseName, options))
        {
            var destination = RequestHandler.Database.Smuggler.CreateDestination();
            var smuggler = RequestHandler.Database.Smuggler.Create(source, destination, jsonOperationContext, options);
            await smuggler.ExecuteAsync();
        }
    }
}
