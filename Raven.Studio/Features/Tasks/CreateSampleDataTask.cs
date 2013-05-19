using System;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Documents;
using System.Windows.Ink;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Animation;
using System.Windows.Shapes;
using Raven.Abstractions.Commands;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Indexing;
using Raven.Client.Connection.Async;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;

namespace Raven.Studio.Features.Tasks
{
    public class CreateSampleDataTask : DatabaseTask
    {
        public CreateSampleDataTask(IAsyncDatabaseCommands databaseCommands, string databaseName) : base(databaseCommands, "Create Sample Data", databaseName)
        {
        }

        protected async override Task<DatabaseTaskOutcome> RunImplementation()
        {
            var statistics = await DatabaseCommands.GetStatisticsAsync();
            if (statistics.CountOfDocuments > 0)
            {
                ReportError("Database already contains documents");
                return DatabaseTaskOutcome.Error;
            }

            Report("Creating Sample Data, Please wait...");

            // this code assumes a small enough dataset, and doesn't do any sort
            // of paging or batching whatsoever.

            using (var sampleData = typeof(CreateSampleDataTask).Assembly.GetManifestResourceStream("Raven.Studio.Assets.EmbeddedData.MvcMusicStore_Dump.json"))
            using (var streamReader = new StreamReader(sampleData))
            {
                Report("Reading documents");

                var musicStoreData = (RavenJObject)RavenJToken.ReadFrom(new JsonTextReader(streamReader));
                foreach (var index in musicStoreData.Value<RavenJArray>("Indexes"))
                {
                    var indexName = index.Value<string>("name");
                    var ravenJObject = index.Value<RavenJObject>("definition");
                    Report("Adding index " + indexName);
                    var putDoc = DatabaseCommands.PutIndexAsync(indexName, ravenJObject.JsonDeserialization<IndexDefinition>(), true);
                    await putDoc;
                }

                Report("Storing documents");

                await DatabaseCommands.BatchAsync(
                    musicStoreData.Value<RavenJArray>("Docs").OfType<RavenJObject>().Select(
                        doc =>
                        {
                            var metadata = doc.Value<RavenJObject>("@metadata");
                            doc.Remove("@metadata");
                            return new PutCommandData
                            {
                                Document = doc,
                                Metadata = metadata,
                                Key = metadata.Value<string>("@id"),
                            };
                        }).ToArray()
                    );
            }

            return DatabaseTaskOutcome.Succesful;
        }
    }
}
