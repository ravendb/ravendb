using System;
using System.Threading.Tasks;
using Raven.Server.Documents.Handlers.Processors.Identities;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers
{
    public class IdentityHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/identity/next", "POST", AuthorizationStatus.ValidUser, EndpointType.Write)]
        public async Task NextIdentityFor()
        {
            var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");

            if (name[name.Length - 1] != '|')
                name += '|';

            var (_, _, newIdentityValue) = await Database.ServerStore.GenerateClusterIdentityAsync(name, Database.IdentityPartsSeparator, Database.Name, GetRaftRequestIdFromQuery());

            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                writer.WriteStartObject();

                writer.WritePropertyName("NewIdentityValue");
                writer.WriteInteger(newIdentityValue);

                writer.WriteEndObject();
            }
        }

        [RavenAction("/databases/*/identity/seed", "POST", AuthorizationStatus.ValidUser, EndpointType.Write)]
        public async Task SeedIdentityFor()
        {
            using (var processor = new IdentityHandlerProcessorForPostIdentity(this))
                await processor.ExecuteAsync();
        }
    }
}
