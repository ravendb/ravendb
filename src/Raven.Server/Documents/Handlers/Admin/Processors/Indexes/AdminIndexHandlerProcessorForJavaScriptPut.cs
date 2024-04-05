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
        if (HttpContext.Features.Get<IHttpAuthenticationFeature>() is RavenServer.AuthenticateConnection feature &&
            feature.CanAccess(RequestHandler.DatabaseName, requireAdmin: true, requireWrite: true) == false)
        {
            throw new UnauthorizedAccessException("Deploying indexes from legacy replication requires admin privileges.");
        }

        using (ContextPool.AllocateOperationContext(out JsonOperationContext jsonOperationContext))
        await using (var stream = new ArrayStream(RequestHandler.RequestBodyStream(), nameof(DatabaseItemType.Indexes)))
        using (var source = new StreamSource(stream, jsonOperationContext, RequestHandler.DatabaseName))
        {
            var destination = RequestHandler.Database.Smuggler.CreateDestination();
            var options = new DatabaseSmugglerOptionsServerSide
            {
                OperateOnTypes = DatabaseItemType.Indexes
            };

            var smuggler = RequestHandler.Database.Smuggler.Create(source, destination, jsonOperationContext, options);
            await smuggler.ExecuteAsync();
        }
    }
}
