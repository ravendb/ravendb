using System;
using System.Net;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Microsoft.AspNetCore.Http.Features.Authentication;
using Raven.Client;
using Raven.Client.Exceptions;
using Raven.Client.ServerWide;
using Raven.Server.Config;
using Raven.Server.Config.Attributes;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Admin.Processors.Configuration;

internal abstract class AbstractAdminConfigurationHandlerProcessorForGetSettings<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext 
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    protected AbstractAdminConfigurationHandlerProcessorForGetSettings([NotNull] TRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected abstract RavenConfiguration GetDatabaseConfiguration();

    public override async ValueTask ExecuteAsync()
    {
        ConfigurationEntryScope? scope = null;
        var scopeAsString = RequestHandler.GetStringQueryString("scope", required: false);
        if (scopeAsString != null)
        {
            if (Enum.TryParse<ConfigurationEntryScope>(scopeAsString, ignoreCase: true, out var value) == false)
                throw new BadRequestException($"Could not parse '{scopeAsString}' to a valid configuration entry scope.");

            scope = value;
        }

        var feature = HttpContext.Features.Get<IHttpAuthenticationFeature>() as RavenServer.AuthenticateConnection;
        var status = feature?.Status ?? RavenServer.AuthenticationStatus.ClusterAdmin;

        DatabaseRecord databaseRecord;

        using (RequestHandler.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
        {
            var dbId = Constants.Documents.Prefix + RequestHandler.DatabaseName;
            using (context.OpenReadTransaction())
            using (var dbDoc = RequestHandler.ServerStore.Cluster.Read(context, dbId, out long etag))
            {
                if (dbDoc == null)
                {
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                    return;
                }

                databaseRecord = JsonDeserializationCluster.DatabaseRecord(dbDoc);
            }
        }

        var settingsResult = new SettingsResult();

        foreach (var configurationEntryMetadata in RavenConfiguration.AllConfigurationEntries.Value)
        {
            if (scope.HasValue && scope != configurationEntryMetadata.Scope)
                continue;

            var entry = new ConfigurationEntryDatabaseValue(GetDatabaseConfiguration(), databaseRecord, configurationEntryMetadata, status);
            settingsResult.Settings.Add(entry);
        }

        using (RequestHandler.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
        {
            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
            {
                context.Write(writer, settingsResult.ToJson());
            }
        }
    }
}
