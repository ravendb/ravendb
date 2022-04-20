using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Voron;
using Voron.Debugging;

namespace Raven.Server.Documents.Handlers.Processors.Debugging
{
    internal class StorageHandlerProcessorForGetReport : AbstractHandlerProcessor<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public StorageHandlerProcessorForGetReport([NotNull] DatabaseRequestHandler requestHandler)
            : base(requestHandler, requestHandler.ContextPool)
        {
        }

        public override async ValueTask ExecuteAsync()
        {
            await GetStorageReport();
        }

        private async ValueTask GetStorageReport()
        {
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
                {
                    writer.WriteStartObject();

                    writer.WritePropertyName("BasePath");
                    writer.WriteString(RequestHandler.Database.Configuration.Core.DataDirectory.FullPath);
                    writer.WriteComma();

                    writer.WritePropertyName("Results");
                    writer.WriteStartArray();
                    var first = true;
                    foreach (var env in RequestHandler.Database.GetAllStoragesEnvironment())
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

                        var djv = (DynamicJsonValue)TypeConverter.ToBlittableSupportedType(GetReport(env));
                        writer.WritePropertyName("Report");
                        writer.WriteObject(context.ReadObject(djv, env.Name));

                        writer.WriteEndObject();
                    }

                    writer.WriteEndArray();

                    writer.WriteEndObject();
                }
            }
        }

        private static StorageReport GetReport(StorageEnvironmentWithType environment)
        {
            using (var tx = environment.Environment.ReadTransaction())
            {
                return environment.Environment.GenerateReport(tx);
            }
        }
    }
}
