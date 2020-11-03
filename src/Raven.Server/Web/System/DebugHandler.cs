using System.Linq;
using System.Threading.Tasks;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Web.System
{
    public sealed class DebugHandler : RequestHandler
    {
        [RavenAction("/debug/routes", "GET", AuthorizationStatus.ValidUser)]
        public async Task Routes()
        {
            var debugRoutes = Server.Router.AllRoutes
                .Where(x => x.IsDebugInformationEndpoint)
                .GroupBy(x => x.Path)
                .OrderBy(x => x.Key);

            var productionRoutes = Server.Router.AllRoutes
              .Where(x => x.IsDebugInformationEndpoint == false)
                .GroupBy(x => x.Path)
                .OrderBy(x => x.Key);

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                writer.WriteStartObject();
                writer.WritePropertyName("Debug");
                writer.WriteStartArray();
                var first = true;
                foreach (var route in debugRoutes)
                {
                    if (first == false)
                    {
                        writer.WriteComma();
                    }
                    first = false;

                    writer.WriteStartObject();
                    writer.WritePropertyName("Path");
                    writer.WriteString(route.Key);
                    writer.WriteComma();
                    writer.WritePropertyName("Methods");
                    writer.WriteString(string.Join(", ", route.Select(x => x.Method)));
                    writer.WriteEndObject();
                }
                writer.WriteEndArray();

                writer.WriteComma();
                writer.WritePropertyName("Production");
                writer.WriteStartArray();
                first = true;
                foreach (var route in productionRoutes)
                {
                    if (first == false)
                    {
                        writer.WriteComma();
                    }
                    first = false;

                    writer.WriteStartObject();
                    writer.WritePropertyName("Path");
                    writer.WriteString(route.Key);
                    writer.WriteComma();
                    writer.WritePropertyName("Methods");
                    writer.WriteString(string.Join(", ", route.Select(x => x.Method)));
                    writer.WriteEndObject();
                }
                writer.WriteEndArray();

                writer.WriteEndObject();
            }
        }
    }
}
