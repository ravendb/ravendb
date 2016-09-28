using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Raven.Server.Documents;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Web.Operations
{
    public class OperationAlertsHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/operation/alerts", "GET")]
        [RavenAction("/operation/alerts", "GET")]
        public Task Get()
        {
            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            {
                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteStartObject();
                    writer.WritePropertyName("Global");
                    ServerStore.Alerts.ReadAlerts(writer);
                    writer.WriteComma();
                    writer.WritePropertyName("Local");
                    Database.Alerts.ReadAlerts(writer);
                    writer.WriteEndObject();
                }
            }

            return Task.CompletedTask;
        }

    }
}