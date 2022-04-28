using JetBrains.Annotations;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.Identities
{
    internal class IdentityDebugHandlerProcessorForGetIdentities : AbstractIdentityDebugHandlerProcessorForGetIdentities<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public IdentityDebugHandlerProcessorForGetIdentities([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }
    }
}
