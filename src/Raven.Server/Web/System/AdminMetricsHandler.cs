// -----------------------------------------------------------------------
//  <copyright file="AdminMetricsHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Threading.Tasks;
using Raven.Server.Routing;
using Sparrow.Json;

namespace Raven.Server.Web.System
{
    public class AdminMetricsHandler : RequestHandler
    {
        [RavenAction("/admin/metrics", "GET", AuthorizationStatus.Operator)]
        public async Task GetRootStats()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                context.Write(writer, Server.Metrics.ToJson());
            }
        }
    }
}
