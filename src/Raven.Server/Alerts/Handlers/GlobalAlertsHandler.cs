using System.Threading.Tasks;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Alerts.Handlers
{
    public class GlobalAlertsHandler : RequestHandler
    {
        [RavenAction("/alerts", "GET")]
        public Task Get()
        {
            JsonOperationContext context;
            using (ServerStore.ContextPool.AllocateOperationContext(out context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                ServerStore.Alerts.WriteAlerts(writer);
            }

            return Task.CompletedTask;
        }
    }
}
