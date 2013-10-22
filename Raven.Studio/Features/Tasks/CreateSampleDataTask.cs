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
using Raven.Abstractions.Smuggler;
using Raven.Abstractions.Util;
using Raven.Client.Connection.Async;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;
using Raven.Studio.Features.Smuggler;

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

            using (var sampleData = typeof(CreateSampleDataTask).Assembly.GetManifestResourceStream("Raven.Studio.Assets.EmbeddedData.Northwind.dump"))
            {
                Report("Reading documents");

                var smuggler = new SmugglerApi(DatabaseCommands, s => Report(s));
                await smuggler.ImportData(new SmugglerOptions
                {
                    OperateOnTypes = ItemType.Documents | ItemType.Indexes | ItemType.Transformers,
                    ShouldExcludeExpired = false,
                    BackupStream = sampleData,
                });
            }

            return DatabaseTaskOutcome.Succesful;
        }

		public override void OnError()
		{

		}
    }
}
