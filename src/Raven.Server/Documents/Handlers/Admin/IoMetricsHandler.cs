using System.Diagnostics;
using System.Threading.Tasks;
using Raven.Server.Routing;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;

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
                IoMetrics forGeneralInfo = null;

                writer.WriteStartObject();
                writer.WritePropertyName(context.GetLazyStringFor("Environments"));
                writer.WriteStartArray();
                bool first = true;
                foreach (var storageEnvironment in Database.GetAllStoragesEnvironment())
                {
                    if (storageEnvironment == null || storageEnvironment.Options.IoMetrics == null)
                        continue;

                    if (forGeneralInfo == null)
                        forGeneralInfo = storageEnvironment.Options.IoMetrics;

                    if (first == false)
                    {
                        writer.WriteComma();
                    }
                    first = false;

                    writer.WriteStartObject();
                    writer.WritePropertyName(context.GetLazyStringFor("StoragePath"));
                    writer.WriteString(context.GetLazyStringFor($"{storageEnvironment.Options.BasePath}"));

                    writer.WriteComma();

                    writer.WritePropertyName(context.GetLazyStringFor("SummerizedItems"));
                    writer.WriteStartArray();

                    bool firstSubItemSummerized = true;
                    foreach (var item in storageEnvironment.Options.IoMetrics.GetAllSummerizedItems())
                    {
                        if (firstSubItemSummerized == false)
                        {
                            writer.WriteComma();
                        }
                        firstSubItemSummerized = false;

                        double actualRate = 0;
                        double overallRate = 0;

                        double freq = Stopwatch.Frequency;
                        if (item.TotalTime != 0 && item.TotalTime/freq > 0) // dev by zero check
                            actualRate = item.TotalSize/(item.TotalTime/freq)/(1024D*1024);
                        if (item.TotalTime != 0 && (item.TotalTimeEnd - item.TotalTimeStart)/freq > 0)
                            // dev by zero check
                            overallRate = item.TotalSize/((item.TotalTimeEnd - item.TotalTimeStart)/freq)/(1024D*1024);

                        context.Write(writer, new DynamicJsonValue
                        {
                            ["ActualRate"] = $"{actualRate:0,0.000}",
                            ["OverallRate"] = $"{overallRate:0,0.000}",
                            ["TotalSize"] = $"{item.TotalSize:0,0}",
                            ["TotalTime"] = $"{item.TotalTime:0,0}",
                            ["MinTime"] = $"{item.MinTime:0,0}",
                            ["MaxTime"] = $"{item.MaxTime:0,0}",
                            ["TotalTimeStart"] = $"{item.TotalTimeStart:0,0}",
                            ["TotalTimeEnd"] = $"{item.TotalTimeEnd:0,0}",
                            ["Count"] = $"{item.Count:0,0}"
                        });
                    }

                    writer.WriteEndArray();
                    writer.WriteComma();

                    writer.WritePropertyName(context.GetLazyStringFor("CurrentIoMeterData"));
                    writer.WriteStartArray();

                    bool firstSubItemCurrent = true;
                    foreach (var item in storageEnvironment.Options.IoMetrics.GetAllCurrentItems())
                    {
                        if (firstSubItemCurrent == false)
                        {
                            writer.WriteComma();
                        }
                        firstSubItemCurrent = false;

                        double actualRate = 0;
                        double freq = Stopwatch.Frequency;
                        if (item.Duration != 0 && item.Duration/freq > 0) // dev by zero check
                            actualRate = item.Size/(item.Duration/freq)/(1024D*1024);

                        context.Write(writer, new DynamicJsonValue
                        {
                            ["Rate"] = $"{actualRate:0,0.000}",
                            ["Size"] = $"{item.Size:0,0}",
                            ["Duration"] = $"{item.Duration:0,0}"
                        });
                    }

                    writer.WriteEndArray();
                    writer.WriteEndObject();
                }

                writer.WriteEndArray();

                writer.WriteComma();
                writer.WritePropertyName(context.GetLazyStringFor("GeneralInfo"));

                if (forGeneralInfo != null)
                {
                    context.Write(writer, new DynamicJsonValue
                    {
                        ["BufferSize"] = $"{forGeneralInfo.BuffSize:0,0.000}",
                        ["SummeryBufferSize"] = $"{forGeneralInfo.SummaryBuffSize:0,0}",
                        ["Frequency"] = $"{Stopwatch.Frequency:0,0}"
                    });
                }
                writer.WriteEndObject();
            }
            return Task.CompletedTask;
        }
    }
}
