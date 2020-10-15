using System;
using System.Collections.Generic;
using System.IO.Compression;
using System.Threading.Tasks;
using Microsoft.Extensions.Primitives;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Documents.Smuggler;
using Raven.Server.Documents;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Raven.Server.Smuggler.Documents;
using Raven.Server.Smuggler.Documents.Data;
using DatabaseSmuggler = Raven.Server.Smuggler.Documents.DatabaseSmuggler;

namespace Raven.Server.Web.Studio
{
    public class SampleDataHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/studio/sample-data", "POST", AuthorizationStatus.ValidUser)]
        public async Task PostCreateSampleData()
        {
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                using (context.OpenReadTransaction())
                {
                    foreach (var collection in Database.DocumentsStorage.GetCollections(context))
                    {
                        if (collection.Count > 0)
                        {
                            throw new InvalidOperationException("You cannot create sample data in a database that already contains documents");
                        }
                    }
                }

                var operateOnTypesAsString = GetStringValuesQueryString("operateOnTypes", required: false);
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
                    }, Database.Name, GetRaftRequestIdFromQuery() + "/revisions");
                    var (index, _) = await ServerStore.SendToLeaderAsync(editRevisions);
                    await Database.RachisLogIndexNotifications.WaitForIndexNotification(index, Database.ServerStore.Engine.OperationTimeout);
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
                    var editTimeSeries = new EditTimeSeriesConfigurationCommand(tsConfig, Database.Name, GetRaftRequestIdFromQuery() + "/time-series");
                    var (index, _) = await ServerStore.SendToLeaderAsync(editTimeSeries);
                    await Database.RachisLogIndexNotifications.WaitForIndexNotification(index, Database.ServerStore.Engine.OperationTimeout);
                }

                using (var sampleData = typeof(SampleDataHandler).Assembly
                    .GetManifestResourceStream("Raven.Server.Web.Studio.EmbeddedData.Northwind.ravendbdump"))
                {
                    using (var stream = new GZipStream(sampleData, CompressionMode.Decompress))
                    using (var source = new StreamSource(stream, context, Database))
                    {
                        var destination = new DatabaseDestination(Database);

                        var smuggler = new DatabaseSmuggler(Database, source, destination, Database.Time,
                            options: new DatabaseSmugglerOptionsServerSide
                            {
                                OperateOnTypes = operateOnTypes,
                                SkipRevisionCreation = true
                            });

                        smuggler.Execute();
                    }
                }

                await NoContent();
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

        [RavenAction("/databases/*/studio/sample-data/classes", "GET", AuthorizationStatus.ValidUser)]
        public async Task GetSampleDataClasses()
        {
            using (var sampleData = typeof(SampleDataHandler).Assembly.GetManifestResourceStream("Raven.Server.Web.Studio.EmbeddedData.NorthwindModel.cs"))
            using (var responseStream = ResponseBodyStream())
            {
                HttpContext.Response.ContentType = "text/plain";
                await sampleData.CopyToAsync(responseStream);
            }
        }
    }
}
