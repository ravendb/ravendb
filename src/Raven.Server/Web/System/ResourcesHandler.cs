using System.Threading.Tasks;
using Microsoft.AspNet.WebSockets.Protocol;
using Raven.Server.Json;
using Raven.Server.Json.Parsing;
using Raven.Server.Routing;
using Constants = Raven.Abstractions.Data.Constants;

namespace Raven.Server.Web.System
{
    public class ResourcesHandler : RequestHandler
    {
        [RavenAction("/databases", "GET")]
        public Task Databases()
        {
            return ReturnResources("db/");
        }

        [RavenAction("/fs", "GET")]
        public Task FileSystems()
        {

            return ReturnResources("fs/");
        }

        [RavenAction("/cs", "GET")]
        public Task Counters()
        {
            return ReturnResources("cs/");
        }

        [RavenAction("/ts", "GET")]
        public Task TimeSeries()
        {
            return ReturnResources("ts/");
        }

        private async Task ReturnResources(string prefix)
        {
            RavenOperationContext context;
            using (ServerStore.ContextPool.AllocateOperationContext(out context))
            {
                context.Transaction = context.Environment.ReadTransaction();
                var writer = new BlittableJsonTextWriter(context, ResponseBodyStream());
                writer.WriteStartArray();
                var first = true;
                foreach (var db in ServerStore.StartingWith(context, prefix, GetStart(), GetPageSize()))
                {
                    if(first == false)
                        writer.WriteComma();
                    first = false;

                    //TODO: Actually handle this properly
                    var doc = new DynamicJsonValue
                    {
                        ["Bundles"] = new DynamicJsonArray(),
                        ["Name"] = db.Key.Substring(prefix.Length),
                        ["RejectClientsEnabled"] = false,
                        ["IndexingDisabled"] = false,
                        ["Disabled"] = false,
                        ["IsAdminCurrentTenant"] = true
                    };
                    await context.WriteAsync(writer, doc);
                }
                writer.WriteEndArray();
                writer.Flush();
            }
        }
    }
}