using JetBrains.Annotations;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.Identities;

internal class IdentityHandlerProcessorForNextIdentityFor : AbstractIdentityHandlerProcessorForNextIdentityFor<DatabaseRequestHandler, DocumentsOperationContext>
{
    public IdentityHandlerProcessorForNextIdentityFor([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override char GetDatabaseIdentityPartsSeparator() => RequestHandler.Database.IdentityPartsSeparator;
}
