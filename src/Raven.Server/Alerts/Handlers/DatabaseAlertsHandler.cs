using System.Threading.Tasks;
using Raven.Server.Documents;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Alerts.Handlers
{
    public class DatabaseAlertsHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/alerts", "GET")]
        public Task Get()
        {
            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                Database.Alerts.WriteAlerts(writer);
            }

            return Task.CompletedTask;
        }

    }
}