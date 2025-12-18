using JetBrains.Annotations;
using Raven.Server.Documents.SchemaValidation;
using Raven.Server.Documents.Sharding.Handlers;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.Documents;

internal sealed class ShardedDocumentHandlerProcessorForValidateDocument : AbstractDocumentHandlerProcessorForValidateDocument<ShardedDocumentHandler, TransactionOperationContext>
{
    public ShardedDocumentHandlerProcessorForValidateDocument([NotNull] ShardedDocumentHandler requestHandler) : base(requestHandler)
    {
    }
    protected override SchemaValidatorCache SchemaValidatorCache => RequestHandler.DatabaseContext.SchemaValidationCache;

}
