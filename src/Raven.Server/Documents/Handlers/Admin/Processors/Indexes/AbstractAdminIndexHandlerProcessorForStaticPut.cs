using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Admin.Processors.Indexes;

internal abstract class AbstractAdminIndexHandlerProcessorForStaticPut<TRequestHandler, TOperationContext> : AbstractAdminIndexHandlerProcessorForPut<TRequestHandler, TOperationContext>
    where TRequestHandler : RequestHandler
    where TOperationContext : JsonOperationContext
{
    protected AbstractAdminIndexHandlerProcessorForStaticPut([NotNull] TRequestHandler requestHandler, [NotNull] JsonContextPoolBase<TOperationContext> contextPool)
        : base(requestHandler, contextPool, validatedAsAdmin: true)
    {
    }

    protected abstract ValueTask HandleLegacyIndexesAsync();

    public override async ValueTask ExecuteAsync()
    {
        var isReplicated = RequestHandler.GetBoolValueQueryString("is-replicated", required: false) ?? false;
        if (isReplicated)
        {
            await HandleLegacyIndexesAsync();
            return;
        }

        await base.ExecuteAsync();
    }
}
