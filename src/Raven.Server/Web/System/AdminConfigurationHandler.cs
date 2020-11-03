using System;
using System.Net;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http.Features.Authentication;
using Raven.Client;
using Raven.Client.Exceptions;
using Raven.Server.Config;
using Raven.Server.Config.Attributes;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Web.System
{
    public class AdminConfigurationHandler : ServerRequestHandler
    {
        [RavenAction("/admin/configuration/settings", "GET", AuthorizationStatus.ClusterAdmin)]
        public async Task GetSettings()
        {
            ConfigurationEntryScope? scope = null;
            var scopeAsString = GetStringQueryString("scope", required: false);
            if (scopeAsString != null)
            {
                if (Enum.TryParse<ConfigurationEntryScope>(scopeAsString, ignoreCase: true, out var value) == false)
                    throw new BadRequestException($"Could not parse '{scopeAsString}' to a valid configuration entry scope.");

                scope = value;
            }

            var feature = HttpContext.Features.Get<IHttpAuthenticationFeature>() as RavenServer.AuthenticateConnection;
            var status = feature?.Status ?? RavenServer.AuthenticationStatus.ClusterAdmin;

            var settingsResult = new SettingsResult();

            foreach (var configurationEntryMetadata in RavenConfiguration.AllConfigurationEntries.Value)
            {
                if (scope.HasValue && scope != configurationEntryMetadata.Scope)
                    continue;

                var entry = new ConfigurationEntryServerValue(Server.Configuration.Settings, configurationEntryMetadata, status);
                settingsResult.Settings.Add(entry);
            }

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    context.Write(writer, settingsResult.ToJson());
                }
            }
        }

        [RavenAction("/admin/configuration/studio", "PUT", AuthorizationStatus.Operator)]
        public async Task PutStudioConfiguration()
        {
            await ServerStore.EnsureNotPassiveAsync();

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
            {
                var studioConfigurationJson = await ctx.ReadForDiskAsync(RequestBodyStream(), Constants.Configuration.StudioId);

                var studioConfiguration = JsonDeserializationServer.ServerWideStudioConfiguration(studioConfigurationJson);

                var res = await ServerStore.PutValueInClusterAsync(new PutServerWideStudioConfigurationCommand(studioConfiguration, GetRaftRequestIdFromQuery()));
                await ServerStore.Cluster.WaitForIndexNotification(res.Index);

                NoContentStatus();

                HttpContext.Response.StatusCode = (int)HttpStatusCode.Created;
            }
        }

        [RavenAction("/configuration/studio", "GET", AuthorizationStatus.ValidUser)]
        public async Task GetStudioConfiguration()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                using (context.OpenReadTransaction())
                {
                    var studioConfigurationJson = ServerStore.Cluster.Read(context, Constants.Configuration.StudioId, out long _);
                    if (studioConfigurationJson == null)
                    {
                        HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                        return;
                    }

                    await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                    {
                        writer.WriteObject(studioConfigurationJson);
                    }
                }
            }
        }

        [RavenAction("/admin/configuration/client", "PUT", AuthorizationStatus.Operator)]
        public async Task PutClientConfiguration()
        {
            await ServerStore.EnsureNotPassiveAsync();

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
            {
                var clientConfigurationJson = await ctx.ReadForMemoryAsync(RequestBodyStream(), Constants.Configuration.ClientId);

                var clientConfiguration = JsonDeserializationServer.ClientConfiguration(clientConfigurationJson);
                var res = await ServerStore.PutValueInClusterAsync(new PutClientConfigurationCommand(clientConfiguration, GetRaftRequestIdFromQuery()));
                await ServerStore.Cluster.WaitForIndexNotification(res.Index);

                NoContentStatus();

                HttpContext.Response.StatusCode = (int)HttpStatusCode.Created;
            }
        }

        [RavenAction("/configuration/client", "GET", AuthorizationStatus.ValidUser)]
        public async Task GetClientConfiguration()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                using (context.OpenReadTransaction())
                {
                    var clientConfigurationJson = ServerStore.Cluster.Read(context, Constants.Configuration.ClientId, out long _);
                    if (clientConfigurationJson == null)
                    {
                        HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                        return;
                    }

                    await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                    {
                        writer.WriteObject(clientConfigurationJson);
                    }
                }
            }
        }
    }
}
