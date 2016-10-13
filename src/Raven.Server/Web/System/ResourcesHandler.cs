using System.Threading.Tasks;
using Raven.Client.Data;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

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

        [RavenAction("/resources", "GET")]
        public Task Resources()
        {
            //TODO: fill all required information (see: RavenDB-5438) - return Raven.Client.Data.ResourcesInfo
            TransactionOperationContext context;
            using (ServerStore.ContextPool.AllocateOperationContext(out context))
            {
                context.OpenReadTransaction();
                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteStartObject();

                    writer.WritePropertyName(nameof(ResourcesInfo.Databases));

                    writer.WriteStartArray();
                    var first = true;
                    foreach (var db in ServerStore.StartingWith(context, "db/", GetStart(), GetPageSize()))
                    {
                        if (first == false)
                            writer.WriteComma();
                        first = false;

                        //TODO: Actually handle this properly
                        var doc = new DynamicJsonValue
                        {
                            [nameof(DatabaseInfo.Name)] = db.Key.Substring("db/".Length),
                            [nameof(DatabaseInfo.Disabled)] = false,
                            [nameof(DatabaseInfo.RejectClientsEnabled)] = false,
                            [nameof(DatabaseInfo.IndexingDisabled)] = false
                        };
                        context.Write(writer, doc);
                    }
                    writer.WriteEndArray();

                    //TODO: write fs, cs, ts

                    writer.WriteEndObject();
                }
            }
            return Task.CompletedTask;
        }

        private Task ReturnResources(string prefix)
        {
            TransactionOperationContext context;
            using (ServerStore.ContextPool.AllocateOperationContext(out context))
            {
                context.OpenReadTransaction();
                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteStartArray();
                    var first = true;
                    foreach (var db in ServerStore.StartingWith(context, prefix, GetStart(), GetPageSize()))
                    {
                        if (first == false)
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
                        context.Write(writer, doc);
                    }
                    writer.WriteEndArray();
                }
            }
            return Task.CompletedTask;
        }
    }
}