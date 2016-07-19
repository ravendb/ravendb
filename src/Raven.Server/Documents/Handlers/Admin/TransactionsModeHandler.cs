using System;
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
            if (Enum.TryParse(modeStr, out mode) == false)
                throw new InvalidOperationException("Query string value 'mode' is not a valid mode: " + modeStr);

            var configDuration = TimeSpan.FromMinutes(Database.Configuration.Storage.TransactionsModeDuration);
            var duration = GetTimeSpanQueryString("duration", required: false) ?? configDuration;
            var storageEnvironments = Database.GetAllStoragesEnvironment();

            var rc = 304;
            foreach (var storageEnvironment in storageEnvironments)
            {
                DocumentsOperationContext context;
                using (ContextPool.AllocateOperationContext(out context))
                {
                    using (var tx = context.OpenWriteTransaction())
                    {
                        var result = storageEnvironment?.SetTransactionMode(mode, duration, tx.InnerTransaction.LowLevelTransaction);
                        using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                        {
                            if (result == null)
                            {
                                context.Write(writer, new DynamicJsonValue
                                {
                                    ["Type"] = mode.ToString(),
                                    ["Path"] = storageEnvironment.Options.BasePath,
                                    ["Result"] = "Storage Environment doesn't exists yet"
                                });
                            }
                            else if (result == TransactionsModeResult.ModeAlreadySet)
                            {
                                context.Write(writer, new DynamicJsonValue
                                {
                                    ["Type"] = mode.ToString(),
                                    ["Path"] = storageEnvironment.Options.BasePath,
                                    ["Result"] = "Mode Already Set"
                                });
                            }
                            else if (result == TransactionsModeResult.SetModeSuccessfully)
                            {
                                if (rc == 304)
                                    rc = 200;
                                context.Write(writer, new DynamicJsonValue
                                {
                                    ["Type"] = mode.ToString(),
                                    ["Path"] = storageEnvironment.Options.BasePath,
                                    ["Result"] = "Mode Set Successfully"
                                });
                            }
                            else if (result == TransactionsModeResult.CannotSetMode)
                            {
                                rc = 408; // Request Timeout (Cannot aquire locks)
                                context.Write(writer, new DynamicJsonValue
                                {
                                    ["Type"] = mode.ToString(),
                                    ["Path"] = storageEnvironment.Options.BasePath,
                                    ["Result"] = "Server is Busy. Cannot Aquire Write Lock. Try Later"
                                });
                            }
                            else
                            {
                                rc = 500;
                                context.Write(writer, new DynamicJsonValue
                                {
                                    ["Type"] = mode.ToString(),
                                    ["Path"] = storageEnvironment.Options.BasePath,
                                    ["Result"] = "Undefined/Unhandled Mode"
                                });
                            }
                        }
                    }
                }
            }

            HttpContext.Response.StatusCode = rc;
            return Task.CompletedTask;
        }
    }
}
