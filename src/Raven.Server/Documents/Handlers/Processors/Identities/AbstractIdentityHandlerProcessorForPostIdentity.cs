using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Identities
{
    internal abstract class AbstractIdentityHandlerProcessorForPostIdentity<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
        where TOperationContext : JsonOperationContext 
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    {
        protected AbstractIdentityHandlerProcessorForPostIdentity([NotNull] TRequestHandler requestHandler) : base(requestHandler)
        {
        }

        public override async ValueTask ExecuteAsync()
        {
            var name = RequestHandler.GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");
            var value = RequestHandler.GetLongQueryString("value", true);
            var forced = RequestHandler.GetBoolValueQueryString("force", false) ?? false;
            if (value == null)
                throw new ArgumentException("Query string value 'value' must have a non empty value");

            if (name[name.Length - 1] != '|')
                name += '|';

            var newIdentityValue = await RequestHandler.ServerStore.UpdateClusterIdentityAsync(name, RequestHandler.DatabaseName, value.Value, forced, RequestHandler.GetRaftRequestIdFromQuery());
            using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
            {
                writer.WriteStartObject();

                writer.WritePropertyName("NewSeedValue");
                writer.WriteInteger(newIdentityValue);

                writer.WriteEndObject();
            }
        }
    }
}
