using System.Threading.Tasks;
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
                var identitiesBlittable = Database.ServerStore.Cluster.ReadIdentitiesAsBlittable(context, Database.Name,out _);
                context.Write(ResponseBodyStream(), identitiesBlittable);
            }
            return Task.CompletedTask;
        }
    }
}
