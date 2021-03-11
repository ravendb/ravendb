// -----------------------------------------------------------------------
//  <copyright file="RevisionsHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Net;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Refresh;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers
{
    public class RefreshHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/refresh/config", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task GetRefreshConfig()
        {
            using (Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                RefreshConfiguration refreshConfig;
                using (var recordRaw = Server.ServerStore.Cluster.ReadRawDatabaseRecord(context, Database.Name))
                {
                    refreshConfig = recordRaw?.RefreshConfiguration;
                }

                if (refreshConfig != null)
                {
                    await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                    {
                        context.Write(writer, refreshConfig.ToJson());
                    }
                }
                else
                {
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                }
            }
        }

        [RavenAction("/databases/*/admin/refresh/config", "POST", AuthorizationStatus.DatabaseAdmin)]
        public async Task ConfigRefresh()
        {
            await DatabaseConfigurations(ServerStore.ModifyDatabaseRefresh, "read-refresh-config", GetRaftRequestIdFromQuery());
        }
    }
}
