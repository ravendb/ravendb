using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Extensions;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Debugging
{
    public class IdentityDebugHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/debug/identities", "GET")]
        public Task GetIdentities()
        {
            var start = GetStart();
            var pageSize = GetPageSize();

            using (Database.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                var record = Database.ServerStore.Cluster.ReadDatabase(context, Database.Name);
                var identitiesAsJson = record.Identities
                                             .Skip(start)
                                             .Take(pageSize)
                                             .ToDictionary(x => x.Key, x=> x.Value)
                                             .ToJson();

                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    context.Write(writer, identitiesAsJson);
                    writer.Flush();
                }
            }
            return Task.CompletedTask;
        }
    }
}
