using System;
using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;
using Raven.Abstractions.TimeSeries;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Voron;

namespace Raven.Server.Documents.Handlers.Admin
{
    public class IoMetricsHandler : AdminDatabaseRequestHandler
    {
        [RavenAction("/databases/*/admin/io-metrics", "GET")]
        public Task CommitNonLazyTx()
        {
            JsonOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                writer.WriteStartObject();
                writer.WritePropertyName(context.GetLazyStringFor("Environments"));
                writer.WriteStartArray();
                bool first = true;
                foreach (var storageEnvironment in Database.GetAllStoragesEnvironment())
                {
                    if (storageEnvironment == null || storageEnvironment.Options.IoMetrics == null) // ADIADI :: and here - IoMetrics..
                        continue;

                    if (first == false)
                    {
                        writer.WriteComma();
                    }
                    first = false;

                    bool firstSubItem = true;
                    foreach (var item in storageEnvironment.Options.IoMetrics.CreateSummerizedMeterData())
                    {
                        if (firstSubItem == false)
                        {
                            writer.WriteComma();
                        }
                        firstSubItem = false;

                        context.Write(writer, item);
                    }
                }
                writer.WriteEndArray();
                writer.WriteEndObject();
            }
            return Task.CompletedTask;
        }
    }
}
