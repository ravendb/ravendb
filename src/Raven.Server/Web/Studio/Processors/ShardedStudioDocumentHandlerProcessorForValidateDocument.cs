using JetBrains.Annotations;
using Raven.Server.Documents.SchemaValidation;
using Raven.Server.Documents.Sharding.Handlers;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Web.Studio.Processors;

internal sealed class ShardedStudioDocumentHandlerProcessorForValidateDocument : AbstractStudioDocumentHandlerProcessorForValidateDocument<ShardedDatabaseRequestHandler, TransactionOperationContext>
{
    public ShardedStudioDocumentHandlerProcessorForValidateDocument([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override SchemaValidatorCache SchemaValidatorCache => RequestHandler.DatabaseContext.SchemaValidatorCache;

}
