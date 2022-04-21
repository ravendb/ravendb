using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Identities;

internal abstract class AbstractIdentityHandlerProcessorForNextIdentityFor<TRequestHandler, TOperationContext> : AbstractHandlerProcessor<TRequestHandler, TOperationContext>
    where TRequestHandler : RequestHandler
    where TOperationContext : JsonOperationContext
{
    protected AbstractIdentityHandlerProcessorForNextIdentityFor([NotNull] TRequestHandler requestHandler, [NotNull] JsonContextPoolBase<TOperationContext> contextPool) 
        : base(requestHandler, contextPool)
    {
    }

    protected abstract char GetDatabaseIdentityPartsSeparator();

    protected abstract string GetDatabaseName();

    public override async ValueTask ExecuteAsync()
    {
        var name = RequestHandler.GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");

        if (name[^1] != '|')
            name += '|';

        var (_, _, newIdentityValue) = await RequestHandler.ServerStore.GenerateClusterIdentityAsync(name, GetDatabaseIdentityPartsSeparator(), GetDatabaseName(), RequestHandler.GetRaftRequestIdFromQuery());

        using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
        await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
        {
            writer.WriteStartObject();

            writer.WritePropertyName("NewIdentityValue");
            writer.WriteInteger(newIdentityValue);

            writer.WriteEndObject();
        }
    }
}
