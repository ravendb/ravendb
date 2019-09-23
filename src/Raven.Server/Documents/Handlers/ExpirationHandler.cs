// -----------------------------------------------------------------------
//  <copyright file="RevisionsHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Net;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Expiration;
using Raven.Client.Documents.Operations.Refresh;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers
{
    public class ExpirationHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/expiration/config", "GET", AuthorizationStatus.ValidUser)]
        public Task GetExpirationConfig()
        {
            using (Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                ExpirationConfiguration expirationConfig;
                using (var recordRaw = Server.ServerStore.Cluster.ReadRawDatabaseRecord(context, Database.Name))
                {
                    expirationConfig = recordRaw?.GetExpirationConfiguration();
                }

                if (expirationConfig != null)
                {
                    using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                    {
                        context.Write(writer, expirationConfig.ToJson());
                    }
                }
                else
                {
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                }
            }
            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/admin/expiration/config", "POST", AuthorizationStatus.DatabaseAdmin)]
        public async Task ConfigExpiration()
        {
            await DatabaseConfigurations(ServerStore.ModifyDatabaseExpiration, "read-expiration-config", GetRaftRequestIdFromQuery());
        }

        [RavenAction("/databases/*/refresh/config", "GET", AuthorizationStatus.ValidUser)]
        public Task GetRefreshConfig()
        {
            using (Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                RefreshConfiguration refreshConfig;
                using (var recordRaw = Server.ServerStore.Cluster.ReadRawDatabaseRecord(context, Database.Name))
                {
                    refreshConfig = recordRaw?.GetRefreshConfiguration();
                }

                if (refreshConfig != null)
                {
                    using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                    {
                        context.Write(writer, refreshConfig.ToJson());
                    }
                }
                else
                {
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                }
            }
            return Task.CompletedTask;
        }
        
        [RavenAction("/databases/*/admin/refresh/config", "POST", AuthorizationStatus.DatabaseAdmin)]
        public async Task ConfigRefresh()
        {
            await DatabaseConfigurations(ServerStore.ModifyDatabaseRefresh, "read-refresh-config", GetRaftRequestIdFromQuery());
        }
    }
}
