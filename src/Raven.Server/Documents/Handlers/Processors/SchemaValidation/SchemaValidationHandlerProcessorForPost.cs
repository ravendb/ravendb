using JetBrains.Annotations;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.SchemaValidation;

internal sealed class SchemaValidationHandlerProcessorForPost : AbstractSchemaValidationHandlerProcessorForPost<DatabaseRequestHandler, DocumentsOperationContext>
{
    public SchemaValidationHandlerProcessorForPost([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }
}
