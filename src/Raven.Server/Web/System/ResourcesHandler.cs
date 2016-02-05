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
                foreach (var db in ServerStore.StartingWith(context, prefix, GetStart(), GetPageSize()))
                {
                    db.Data.Modifications = new DynamicJsonValue(db.Data)
                    {
                        ["Id"] = db.Key.Substring(prefix.Length),
                        [Constants.Metadata] = new DynamicJsonValue
                        {
                            ["@id"] = db.Key
                        }
                    };
                    await context.WriteAsync(writer, db.Data);
                }
                writer.WriteEndArray();
                writer.Flush();
            }
        }
    }
}