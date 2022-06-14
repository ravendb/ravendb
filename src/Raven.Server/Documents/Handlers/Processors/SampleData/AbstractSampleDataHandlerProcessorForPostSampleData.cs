using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Microsoft.Extensions.Primitives;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Documents.Smuggler;
using Raven.Server.ServerWide.Commands;
using Raven.Server.Smuggler.Documents;
using Raven.Server.Smuggler.Documents.Data;
using Raven.Server.Web;
using Raven.Server.Web.Studio;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.SampleData
{
    internal abstract class AbstractSampleDataHandlerProcessorForPostSampleData<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
        where TOperationContext : JsonOperationContext
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    {
        protected AbstractSampleDataHandlerProcessorForPostSampleData([NotNull] TRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected abstract ValueTask ExecuteSmugglerAsync(JsonOperationContext context, ISmugglerSource source, Stream sampleData, DatabaseItemType operateOnTypes);

        protected abstract ValueTask<bool> IsDatabaseEmptyAsync();

        public override async ValueTask ExecuteAsync()
        {
            var databaseName = RequestHandler.DatabaseName;

            if (await IsDatabaseEmptyAsync() == false)
                throw new InvalidOperationException("You cannot create sample data in a database that already contains documents");

            var operateOnTypesAsString = RequestHandler.GetStringValuesQueryString("operateOnTypes", required: false);
            var operateOnTypes = GetOperateOnTypes(operateOnTypesAsString);

            if (operateOnTypes.HasFlag(DatabaseItemType.RevisionDocuments))
            {
                var editRevisions = new EditRevisionsConfigurationCommand(new RevisionsConfiguration
                {
                    Collections = new Dictionary<string, RevisionsCollectionConfiguration>
                    {
                        ["Orders"] = new RevisionsCollectionConfiguration
                        {
                            Disabled = false
                        }
                    }
                }, databaseName, RequestHandler.GetRaftRequestIdFromQuery() + "/revisions");
                var (index, _) = await RequestHandler.ServerStore.SendToLeaderAsync(editRevisions);
                await RequestHandler.WaitForIndexNotificationAsync(index);
            }

            if (operateOnTypes.HasFlag(DatabaseItemType.TimeSeries))
            {
                var tsConfig = new TimeSeriesConfiguration
                {
                    NamedValues = new Dictionary<string, Dictionary<string, string[]>>
                    {
                        ["Companies"] = new Dictionary<string, string[]>
                        {
                            ["StockPrices"] = new[] { "Open", "Close", "High", "Low", "Volume" }
                        },
                        ["Employees"] = new Dictionary<string, string[]>
                        {
                            ["HeartRates"] = new[] { "BPM" }
                        }
                    }
                };

                var editTimeSeries = new EditTimeSeriesConfigurationCommand(tsConfig, databaseName, RequestHandler.GetRaftRequestIdFromQuery() + "/time-series");
                var (index, _) = await RequestHandler.ServerStore.SendToLeaderAsync(editTimeSeries);
                await RequestHandler.WaitForIndexNotificationAsync(index);
            }

            using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                await using (var sampleData = typeof(SampleDataHandler).Assembly
                                 .GetManifestResourceStream("Raven.Server.Web.Studio.EmbeddedData.Northwind.ravendbdump"))
                await using (var stream = new GZipStream(sampleData, CompressionMode.Decompress))
                using (var source = new StreamSource(stream, context, databaseName))
                {
                    await ExecuteSmugglerAsync(context, source, sampleData, operateOnTypes);
                }

                await RequestHandler.NoContent();
            }

            static DatabaseItemType GetOperateOnTypes(StringValues operateOnTypesAsString)
            {
                if (operateOnTypesAsString.Count == 0)
                {
                    return DatabaseItemType.Documents
                        | DatabaseItemType.RevisionDocuments
                        | DatabaseItemType.Attachments
                        | DatabaseItemType.CounterGroups
                        | DatabaseItemType.TimeSeries
                        | DatabaseItemType.Indexes
                        | DatabaseItemType.CompareExchange;
                }

                var result = DatabaseItemType.None;
                for (var i = 0; i < operateOnTypesAsString.Count; i++)
                    result |= Enum.Parse<DatabaseItemType>(operateOnTypesAsString[i], ignoreCase: true);

                return result;
            }
        }
    }
}
