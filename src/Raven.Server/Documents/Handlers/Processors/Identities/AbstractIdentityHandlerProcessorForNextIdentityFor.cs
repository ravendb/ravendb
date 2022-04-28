using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Identities;

internal abstract class AbstractIdentityHandlerProcessorForNextIdentityFor<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext 
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    protected AbstractIdentityHandlerProcessorForNextIdentityFor([NotNull] TRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected abstract char GetDatabaseIdentityPartsSeparator();

    public override async ValueTask ExecuteAsync()
    {
        var name = RequestHandler.GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");

        if (name[^1] != '|')
            name += '|';

        var (_, _, newIdentityValue) = await RequestHandler.ServerStore.GenerateClusterIdentityAsync(name, GetDatabaseIdentityPartsSeparator(), RequestHandler.DatabaseName, RequestHandler.GetRaftRequestIdFromQuery());

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
