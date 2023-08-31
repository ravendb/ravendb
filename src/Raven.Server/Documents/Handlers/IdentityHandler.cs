using System.Threading.Tasks;
using Raven.Server.Documents.Handlers.Processors.Identities;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Handlers
{
    public sealed class IdentityHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/identity/next", "POST", AuthorizationStatus.ValidUser, EndpointType.Write)]
        public async Task NextIdentityFor()
        {
            using (var processor = new IdentityHandlerProcessorForNextIdentityFor(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/identity/seed", "POST", AuthorizationStatus.ValidUser, EndpointType.Write)]
        public async Task SeedIdentityFor()
        {
            using (var processor = new IdentityHandlerProcessorForPostIdentity(this))
                await processor.ExecuteAsync();
        }
    }
}
