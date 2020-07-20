using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Server.Routing;
using Raven.Server.Web;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers.Debugging
{
    public class ServerTransactionDebugHandler : RequestHandler
    {
        [RavenAction("/admin/debug/txinfo", "GET", AuthorizationStatus.DatabaseAdmin, IsDebugInformationEndpoint = true)]
        public Task TxInfo()
        {
            var results = new List<TransactionDebugHandler.TransactionInfo>();

            var env = Server.ServerStore._env;
            var txInfo = new TransactionDebugHandler.TransactionInfo
            {
                Path = env.Options.BasePath.FullPath,
                Information = env.ActiveTransactions.AllTransactionsInstances
            };
            results.Add(txInfo);

            using (Server.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                context.Write(writer, new DynamicJsonValue
                {
                    ["tx-info"] = TransactionDebugHandler.ToJson(results)
                });
            }
            return Task.CompletedTask;
        }
    }
}
