using JetBrains.Annotations;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.Identities;

internal class IdentityHandlerProcessorForNextIdentityFor : AbstractIdentityHandlerProcessorForNextIdentityFor<DatabaseRequestHandler, DocumentsOperationContext>
{
    public IdentityHandlerProcessorForNextIdentityFor([NotNull] DatabaseRequestHandler requestHandler) 
        : base(requestHandler, requestHandler.ContextPool)
    {
    }

    protected override char GetDatabaseIdentityPartsSeparator() => RequestHandler.Database.IdentityPartsSeparator;

    protected override string GetDatabaseName() => RequestHandler.Database.Name;
}
