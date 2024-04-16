using JetBrains.Annotations;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Admin.Processors.Indexes;

internal abstract class AbstractAdminIndexHandlerProcessorForStaticPut<TRequestHandler, TOperationContext> : AbstractAdminIndexHandlerProcessorForPut<TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext 
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    protected AbstractAdminIndexHandlerProcessorForStaticPut([NotNull] TRequestHandler requestHandler)
        : base(requestHandler, validatedAsAdmin: true)
    {
    }
}
