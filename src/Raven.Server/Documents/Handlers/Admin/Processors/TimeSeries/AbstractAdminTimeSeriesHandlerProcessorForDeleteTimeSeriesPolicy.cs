using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Server.Documents.Handlers.Processors.TimeSeries;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Admin.Processors.TimeSeries
{
    internal abstract class AbstractAdminTimeSeriesHandlerProcessorForDeleteTimeSeriesPolicy<TRequestHandler, TOperationContext> : AbstractTimeSeriesHandlerProcessor<TRequestHandler, TOperationContext>
        where TRequestHandler : RequestHandler
        where TOperationContext : JsonOperationContext
    {
        public AbstractAdminTimeSeriesHandlerProcessorForDeleteTimeSeriesPolicy([NotNull] TRequestHandler requestHandler, [NotNull] JsonContextPoolBase<TOperationContext> contextPool) : base(requestHandler, contextPool)
        {
        }

        protected abstract string GetDatabaseName();

        public override async ValueTask ExecuteAsync()
        {
            await RequestHandler.ServerStore.EnsureNotPassiveAsync();
            var collection = RequestHandler.GetStringQueryString("collection", required: true);
            var name = RequestHandler.GetStringQueryString("name", required: true);

            using (RequestHandler.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                TimeSeriesConfiguration current;
                using (context.OpenReadTransaction())
                {
                    current = RequestHandler.ServerStore.Cluster.ReadRawDatabaseRecord(context, GetDatabaseName()).TimeSeriesConfiguration;
                }

                if (current?.Collections?.ContainsKey(collection) == true)
                {
                    var p = current.Collections[collection].GetPolicyByName(name, out _);
                    if (p == null)
                        return;

                    if (ReferenceEquals(p, current.Collections[collection].RawPolicy))
                    {
                        current.Collections[collection].RawPolicy = RawTimeSeriesPolicy.Default;
                    }
                    else
                    {
                        current.Collections[collection].Policies.Remove(p);
                    }

                    current.InitializeRollupAndRetention();

                    RequestHandler.ServerStore.LicenseManager.AssertCanAddTimeSeriesRollupsAndRetention(current);

                    var editTimeSeries = new EditTimeSeriesConfigurationCommand(current, GetDatabaseName(), RequestHandler.GetRaftRequestIdFromQuery());
                    var (index, _) = await RequestHandler.ServerStore.SendToLeaderAsync(editTimeSeries);

                    await RequestHandler.WaitForIndexToBeAppliedAsync(context, index);
                    await SendConfigurationResponseAsync(context, index);
                }
            }
        }
    }
}
