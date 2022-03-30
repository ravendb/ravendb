using JetBrains.Annotations;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.Identities
{
    internal class IdentityHandlerProcessorForPostIdentity : AbstractIdentityHandlerProcessorForPostIdentity<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public IdentityHandlerProcessorForPostIdentity([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler, requestHandler.ContextPool)
        {
        }

        protected override string GetDatabaseName() => RequestHandler.Database.Name;
    }
}
