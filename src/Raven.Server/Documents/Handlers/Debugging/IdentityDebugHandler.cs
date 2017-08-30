using System.Threading.Tasks;
using Raven.Client;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Debugging
{
    public class IdentityDebugHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/debug/identities", "GET", AuthorizationStatus.ValidUser, IsDebugInformationEndpoint = true)]
        public Task GetIdentities()
        {
            using (Database.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                var json = Database.ServerStore.Cluster.Read(context, Constants.Documents.IdentitiesPrefix + Database.Name.ToLowerInvariant());
                context.Write(ResponseBodyStream(), json);
            }

            return Task.CompletedTask;
        }
    }
}
