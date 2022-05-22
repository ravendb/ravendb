using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Subscriptions;
using Raven.Server.Documents.Handlers.Processors.Subscriptions;
using Raven.Server.Documents.Sharding.Operations;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Utils;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Subscriptions
{
    internal class ShardedSubscriptionsHandlerProcessorForTrySubscription : AbstractSubscriptionsHandlerProcessorForTrySubscription<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedSubscriptionsHandlerProcessorForTrySubscription([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override async ValueTask TryoutSubscriptionAsync(TransactionOperationContext context, SubscriptionConnection.ParsedSubscription subscription, SubscriptionTryout tryout, int pageSize)
        {
            var timeLimit = RequestHandler.GetIntValueQueryString("timeLimit", false);
            var result = await RequestHandler.ShardExecutor.ExecuteParallelForAllAsync(new ShardedSubscriptionTryoutOperation(HttpContext, context, tryout, pageSize, timeLimit));
            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
            {
                writer.WriteStartObject();
                writer.WritePropertyName("Results");
                writer.WriteStartArray();
                var numberOfDocs = 0;
                var f = true;

                foreach (var res in result.Results)
                {
                    if (numberOfDocs == pageSize)
                        break;

                    if (res is not BlittableJsonReaderObject bjro)
                        continue;

                    using (bjro)
                    {
                        if (f == false)
                            writer.WriteComma();

                        f = false;
                        WriteBlittable(bjro, writer);
                        numberOfDocs++;
                    }
                }

                writer.WriteEndArray();
                writer.WriteComma();
                writer.WritePropertyName("Includes");
                DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Egor, DevelopmentHelper.Severity.Major, "https://issues.hibernatingrhinos.com/issue/RavenDB-16279");
                writer.WriteStartObject();
                writer.WriteEndObject();
                writer.WriteEndObject();
            }
        }

        private static unsafe void WriteBlittable(BlittableJsonReaderObject bjro, AsyncBlittableJsonTextWriter writer)
        {
            var first = true;

            var prop = new BlittableJsonReaderObject.PropertyDetails();
            writer.WriteStartObject();
            using (var buffers = bjro.GetPropertiesByInsertionOrder())
            {
                for (var i = 0; i < buffers.Size; i++)
                {
                    bjro.GetPropertyByIndex(buffers.Properties[i], ref prop);
                    if (first == false)
                    {
                        writer.WriteComma();
                    }

                    first = false;
                    writer.WritePropertyName(prop.Name);
                    writer.WriteValue(prop.Token & BlittableJsonReaderBase.TypesMask, prop.Value);
                }
            }
            writer.WriteEndObject();
        }
    }
}
