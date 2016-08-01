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
    public class TransactionsModeHandler : AdminDatabaseRequestHandler
    {
        [RavenAction("/databases/*/admin/transactions-mode", "GET")]
        public Task CommitNonLazyTx()
        {
            var modeStr = GetQueryStringValueAndAssertIfSingleAndNotEmpty("mode");
            TransactionsMode mode;
            if (Enum.TryParse(modeStr, true,  out mode) == false)
                throw new InvalidOperationException("Query string value 'mode' is not a valid mode: " + modeStr);

            var configDuration = TimeSpan.FromMinutes(Database.Configuration.Storage.TransactionsModeDuration);
            var duration = GetTimeSpanQueryString("duration", required: false) ?? configDuration;
            JsonOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                writer.WriteStartObject();
                writer.WritePropertyName(("Environments"));
                writer.WriteStartArray();
                bool first = true;
                foreach (var storageEnvironment in Database.GetAllStoragesEnvironment())
                {
                    if (storageEnvironment == null)
                        continue;

                    if (first == false)
                    {
                        writer.WriteComma();
                    }
                    first = false;

                    var result = storageEnvironment.SetTransactionMode(mode, duration);
                    switch (result)
                    {
                        case TransactionsModeResult.ModeAlreadySet:
                            context.Write(writer, new DynamicJsonValue
                            {
                                ["Type"] = mode.ToString(),
                                ["Path"] = storageEnvironment.Options.BasePath,
                                ["Result"] = "Mode Already Set"
                            });
                            break;
                        case TransactionsModeResult.SetModeSuccessfully:
                            context.Write(writer, new DynamicJsonValue
                            {
                                ["Type"] = mode.ToString(),
                                ["Path"] = storageEnvironment.Options.BasePath,
                                ["Result"] = "Mode Set Successfully"
                            });
                            break;
                        default:
                            throw new ArgumentOutOfRangeException("Result is unexpected value: " + result);
                    }
                }
                writer.WriteEndArray();
                writer.WriteEndObject();
            }
            return Task.CompletedTask;
        }
    }
}
