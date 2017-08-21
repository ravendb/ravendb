using System.Threading.Tasks;
using Raven.Server.Documents;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Web.Studio
{
    public class StudioCustomFunctionsHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/studio/custom-functions", "GET", AuthorizationStatus.ValidUser)]
        public Task GetCustomFunctions()//TODO: remove me
        {
            using (Database.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    context.Write(writer, new DynamicJsonValue
                    {
                        [nameof(CustomFunctions.Functions)] = null
                    });
                }
            }

            return Task.CompletedTask;
        }

    }
}
