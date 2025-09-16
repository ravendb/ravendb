using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Server.Routing;
using Raven.Server.Utils;
using Raven.Server.Web;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Voron.Impl;

namespace Raven.Server.Documents.Handlers.Debugging
{
    public sealed class ServerTransactionDebugHandler : ServerRequestHandler
    {
        [RavenAction("/admin/debug/txinfo", "GET", AuthorizationStatus.Operator, IsDebugInformationEndpoint = true)]
        public async Task TxInfo()
        {
            var results = new List<TransactionDebugHandler.TransactionInfo>();

            var env = Server.ServerStore._env;
            var txInfo = new TransactionDebugHandler.TransactionInfo
            {
                Path = env.Options.BasePath.FullPath,
                Information = env.ActiveTransactions.AllTransactionsInstances.Select(TransactionDebugHandler.ToTxInfoResult).ToList()
            };
            results.Add(txInfo);

            using (Server.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriterForDebug(context, ServerStore, ResponseBodyStream()))
            {
                context.Write(writer, new DynamicJsonValue
                {
                    ["tx-info"] = TransactionDebugHandler.ToJson(results)
                });
            }
        }
    }
}
