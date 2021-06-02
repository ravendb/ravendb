// -----------------------------------------------------------------------
//  <copyright file="AdminCpuCreditsHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Threading.Tasks;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Web.System
{
    public class AdminCpuCreditsHandler : RequestHandler
    {
        [RavenAction("/admin/cpu-credits", "POST", AuthorizationStatus.ClusterAdmin)]
        public async Task UpdateCpuCredits()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
            using (var newCredits = await ctx.ReadForMemoryAsync(RequestBodyStream(), "cpu-credits"))
            {
                var updated = JsonDeserializationServer.CpuCredits(newCredits);
                Server.CpuCreditsBalance.RemainingCpuCredits = updated.RemainingCredits;
            }
        }

        [RavenAction("/admin/cpu-credits/sync", "POST", AuthorizationStatus.ClusterAdmin)]
        public Task SyncCpuCredits()
        {
            Server.ForceSyncCpuCredits();
            return Task.CompletedTask;
        }

        [RavenAction("/debug/cpu-credits", "GET", AuthorizationStatus.ValidUser, EndpointType.Read, IsDebugInformationEndpoint = true)]
        public async Task GetCpuCredits()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                var json = Server.CpuCreditsBalance.ToJson();
                writer.WriteObject(context.ReadObject(json, "cpu/credits"));
            }
        }

        public class CpuCredits
        {
            public double RemainingCredits;

            public DynamicJsonValue ToJson()
            {
                return new DynamicJsonValue
                {
                    [nameof(RemainingCredits)] = RemainingCredits
                };
            }
        }
    }
}
