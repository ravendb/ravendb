using System;
using System.Linq;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Server.Json;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.TimeSeries
{
    internal abstract class AbstractTimeSeriesHandlerProcessorForPostTimeSeriesNamesConfiguration<TRequestHandler, TOperationContext> : AbstractTimeSeriesHandlerProcessor<TRequestHandler, TOperationContext>
        where TOperationContext : JsonOperationContext 
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    {
        public AbstractTimeSeriesHandlerProcessorForPostTimeSeriesNamesConfiguration([NotNull] TRequestHandler requestHandler) : base(requestHandler)
        {
        }

        public override async ValueTask ExecuteAsync()
        {
            await RequestHandler.ServerStore.EnsureNotPassiveAsync();

            using (RequestHandler.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (var json = await context.ReadForDiskAsync(RequestHandler.RequestBodyStream(), "time-series value names"))
            {
                var parameters = JsonDeserializationServer.Parameters.TimeSeriesValueNamesParameters(json);
                parameters.Validate();

                TimeSeriesConfiguration current;
                using (context.OpenReadTransaction())
                {
                    current = RequestHandler.ServerStore.Cluster.ReadRawDatabaseRecord(context, RequestHandler.DatabaseName).TimeSeriesConfiguration ?? new TimeSeriesConfiguration();
                }

                if (current.NamedValues == null)
                    current.AddValueName(parameters.Collection, parameters.TimeSeries, parameters.ValueNames);
                else
                {
                    var currentNames = current.GetNames(parameters.Collection, parameters.TimeSeries);
                    if (currentNames?.SequenceEqual(parameters.ValueNames, StringComparer.Ordinal) == true)
                        return; // no need to update, they identical

                    if (parameters.Update == false)
                    {
                        if (current.TryAddValueName(parameters.Collection, parameters.TimeSeries, parameters.ValueNames) == false)
                            throw new InvalidOperationException(
                                $"Failed to update the names for time-series '{parameters.TimeSeries}' in collection '{parameters.Collection}', they already exists.");
                    }
                    current.AddValueName(parameters.Collection, parameters.TimeSeries, parameters.ValueNames);
                }
                var editTimeSeries = new EditTimeSeriesConfigurationCommand(current, RequestHandler.DatabaseName, RequestHandler.GetRaftRequestIdFromQuery());
                var (index, _) = await RequestHandler.ServerStore.SendToLeaderAsync(editTimeSeries);

                await RequestHandler.WaitForIndexToBeAppliedAsync(context, index);
                await SendConfigurationResponseAsync(context, index);
            }
        }
    }
}
