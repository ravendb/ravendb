using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Server.ServerWide;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Configuration
{
    internal abstract class AbstractConfigurationHandlerProcessorForTimeSeriesConfig<TRequestHandler, TOperationContext> : AbstractHandlerProcessor<TRequestHandler, TOperationContext>
        where TRequestHandler : RequestHandler
        where TOperationContext : JsonOperationContext
    {
        protected AbstractConfigurationHandlerProcessorForTimeSeriesConfig([NotNull] TRequestHandler requestHandler, [NotNull] JsonContextPoolBase<TOperationContext> contextPool)
            : base(requestHandler, contextPool)
        {
        }

        protected abstract string GetDatabaseName();

        public override async ValueTask ExecuteAsync()
        {
            var databaseName = GetDatabaseName();

            await DatabaseRequestHandler.DatabaseConfigurations(
               RequestHandler.ServerStore.ModifyTimeSeriesConfiguration,
               "read-timeseries-config",
               RequestHandler.GetRaftRequestIdFromQuery(),
               databaseName,
               RequestHandler,
               beforeSetupConfiguration: (string name, ref BlittableJsonReaderObject configuration, JsonOperationContext context, ServerStore serverStore) =>
               {
                   if (configuration == null)
                   {
                       return;
                   }

                   var hasCollectionsConfig = configuration.TryGet(nameof(TimeSeriesConfiguration.Collections), out BlittableJsonReaderObject collections) &&
                                              collections?.Count > 0;

                   if (hasCollectionsConfig == false)
                       return;

                   var uniqueKeys = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                   var prop = new BlittableJsonReaderObject.PropertyDetails();

                   for (var i = 0; i < collections.Count; i++)
                   {
                       collections.GetPropertyByIndex(i, ref prop);

                       if (uniqueKeys.Add(prop.Name) == false)
                       {
                           throw new InvalidOperationException("Cannot have two different revision configurations on the same collection. " +
                                                               $"Collection name : '{prop.Name}'");
                       }
                   }
               });
        }
    }
}
