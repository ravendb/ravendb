using System.Threading.Tasks;
using Raven.Server.Routing;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Raven.Server.Web;

namespace Raven.Server.Documents.Handlers.Admin
{
    public class AdminStatsHandler : RequestHandler
    {
        [RavenAction("/admin/stats/server-id", "GET", "/admin/stats/server-id", NoAuthorizationRequired = true)]
        public Task ServerId()
        {
            JsonOperationContext context;

            using (ServerStore.ContextPool.AllocateOperationContext(out context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                context.Write(writer, new DynamicJsonValue
                {
                    ["ServerId"] = ServerStore.GetServerId().ToString(),
                });
            }

            return Task.CompletedTask;
        }
    }
}