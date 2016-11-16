using System.Threading.Tasks;
using Raven.Server.Documents.Indexes.MapReduce;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Voron;
using Voron.Debugging;

namespace Raven.Server.Documents.Handlers.Debugging
{
    public class StorageHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/debug/storage/report", "GET")]
        public Task Report()
        {
            var details = GetBoolValueQueryString("details", required: false) ?? false;

            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            {
                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    var first = true;
                    writer.WriteStartArray();
                    foreach (var env in Database.GetAllStoragesEnvironment())
                    {
                        if (first == false)
                            writer.WriteComma();

                        first = false;

                        writer.WriteStartObject();

                        writer.WritePropertyName("Name");
                        writer.WriteString(env.Name);
                        writer.WriteComma();

                        writer.WritePropertyName("Type");
                        writer.WriteString(env.Type.ToString());
                        writer.WriteComma();

                        var djv = (DynamicJsonValue)TypeConverter.ToBlittableSupportedType(GetReport(env, details));
                        writer.WritePropertyName("Report");
                        writer.WriteObject(context.ReadObject(djv, env.Name));

                        writer.WriteEndObject();
                    }

                    writer.WriteEndArray();
                }
            }

            return Task.CompletedTask;
        }

        private StorageReport GetReport(StorageEnvironmentWithType environment, bool details)
        {
                if (environment.Type != StorageEnvironmentWithType.StorageEnvironmentType.Index)
                {
                    using (var tx = environment.Environment.ReadTransaction())
                    {
                        return environment.Environment.GenerateReport(tx, details);
                    }
                }

            var index = Database.IndexStore.GetIndex(environment.Name);
            return index.GenerateStorageReport(details);
        }
    }
}