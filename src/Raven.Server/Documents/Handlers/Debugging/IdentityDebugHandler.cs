using System.Threading.Tasks;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Debugging
{
    public class IdentityDebugHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/debug/identities", "GET", AuthorizationStatus.ValidUser, IsDebugInformationEndpoint = true)]
        public Task GetIdentities()
        {
            var start = GetStart();
            var pageSize = GetPageSize();

            using (Database.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteStartObject();

                    var first = true;
                    foreach (var identity in Database.ServerStore.Cluster.GetIdentitiesFrom(context, Database.Name, start, pageSize))
                    {
                        if (first == false)
                            writer.WriteComma();

                        first = false;
                        writer.WritePropertyName(identity.Prefix);
                        writer.WriteInteger(identity.Value);
                    }

                    writer.WriteEndObject();
                }
            }

            return Task.CompletedTask;
        }
    }
}
