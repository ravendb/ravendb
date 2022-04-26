using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.Documents.Handlers.Processors.TimeSeries;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Admin.Processors.TimeSeries
{
    internal abstract class AbstractAdminTimeSeriesHandlerProcessorForPutTimeSeriesPolicy<TRequestHandler, TOperationContext> : AbstractTimeSeriesHandlerProcessor<TRequestHandler, TOperationContext>
        where TRequestHandler : RequestHandler
        where TOperationContext : JsonOperationContext
    {
        public AbstractAdminTimeSeriesHandlerProcessorForPutTimeSeriesPolicy([NotNull] TRequestHandler requestHandler, [NotNull] JsonContextPoolBase<TOperationContext> contextPool) : base(requestHandler, contextPool)
        {
        }

        protected abstract string GetDatabaseName();

        public override async ValueTask ExecuteAsync()
        {
            await RequestHandler.ServerStore.EnsureNotPassiveAsync();
            var collection = RequestHandler.GetStringQueryString("collection", required: true);

            using (RequestHandler.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (var json = await context.ReadForDiskAsync(RequestHandler.RequestBodyStream(), "time-series policy config"))
            {
                var policy = JsonDeserializationCluster.TimeSeriesPolicy(json);

                TimeSeriesConfiguration current;
                using (context.OpenReadTransaction())
                {
                    current = RequestHandler.ServerStore.Cluster.ReadRawDatabaseRecord(context, GetDatabaseName()).TimeSeriesConfiguration ?? new TimeSeriesConfiguration();
                }

                current.Collections ??= new Dictionary<string, TimeSeriesCollectionConfiguration>(StringComparer.OrdinalIgnoreCase);

                if (current.Collections.ContainsKey(collection) == false)
                    current.Collections[collection] = new TimeSeriesCollectionConfiguration();

                if (RawTimeSeriesPolicy.IsRaw(policy))
                    current.Collections[collection].RawPolicy = new RawTimeSeriesPolicy(policy.RetentionTime);
                else
                {
                    current.Collections[collection].Policies ??= new List<TimeSeriesPolicy>();
                    var existing = current.Collections[collection].GetPolicyByName(policy.Name, out _);
                    if (existing != null)
                        current.Collections[collection].Policies.Remove(existing);

                    current.Collections[collection].Policies.Add(policy);
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
