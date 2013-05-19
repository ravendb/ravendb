using System;
using System.Collections.Generic;
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
using Raven.Abstractions.Smuggler;
using Raven.Client.Connection.Async;
using Raven.Studio.Features.Input;
using Raven.Studio.Features.Smuggler;
using Raven.Studio.Models;

namespace Raven.Studio.Features.Tasks
{
    public class ImportDatabaseTask : DatabaseTask
    {
        private bool includeAttachements, includeDocuments, includeIndexes, includeTransformers;
        private readonly bool shouldExcludeExpired;
        private readonly int batchSize;
        private readonly string transformScript;
        private readonly List<FilterSetting> filterSettings;

        public ImportDatabaseTask(IAsyncDatabaseCommands databaseCommands, string databaseName, bool includeAttachements, bool includeDocuments, bool includeIndexes,
                                  bool includeTransformers, bool shouldExcludeExpired, int batchSize, string transformScript, List<FilterSetting> filterSettings)
            : base(databaseCommands, "Import Database", databaseName)
        {
            this.includeAttachements = includeAttachements;
            this.includeDocuments = includeDocuments;
            this.includeIndexes = includeIndexes;
            this.includeTransformers = includeTransformers;
            this.shouldExcludeExpired = shouldExcludeExpired;
            this.batchSize = batchSize;
            this.transformScript = transformScript;
            this.filterSettings = filterSettings;
        }

        protected async override Task<DatabaseTaskOutcome> RunImplementation()
        {
            if (ApplicationModel.Current.Server.Value.SelectedDatabase.Value.Statistics.Value != null
                && ApplicationModel.Current.Server.Value.SelectedDatabase.Value.Statistics.Value.CountOfDocuments != 0)
            {
                return await AskUser.ConfirmationWithContinuation("Override Documents?", "There are documents in the database :" +
                                                                  ApplicationModel.Current.Server.Value.SelectedDatabase.Value.Name +
                                                                  "." + Environment.NewLine
                                                                  + "This operation can override those documents.",
                                                                  onOkay: ExecuteInternal, 
                                                                  onCancelled:  () => TaskEx.FromResult(DatabaseTaskOutcome.Abandoned));

            }
            else
            {
                return await ExecuteInternal();
            }
        }

        private async Task<DatabaseTaskOutcome> ExecuteInternal()
        {
            if (includeDocuments == false && includeAttachements == false && includeIndexes == false && includeTransformers == false)
                return DatabaseTaskOutcome.Abandoned;

            var openFile = new OpenFileDialog
            {
                Filter = "Raven Dumps|*.ravendump;*.raven.dump",
            };

            if (openFile.ShowDialog() != true)
                return DatabaseTaskOutcome.Abandoned;

            Report(String.Format("Importing from {0}", openFile.File.Name));

            using (var stream = openFile.File.OpenRead())
            {

                ItemType operateOnTypes = 0;

                if (includeDocuments)
                {
                    operateOnTypes |= ItemType.Documents;
                }

                if (includeAttachements)
                {
                    operateOnTypes |= ItemType.Attachments;
                }

                if (includeIndexes)
                {
                    operateOnTypes |= ItemType.Indexes;
                }

                if (includeTransformers)
                {
                    operateOnTypes |= ItemType.Transformers;
                }

                var smugglerOptions = new SmugglerOptions
                {
                    BatchSize = batchSize,
                    Filters = filterSettings,
                    TransformScript = transformScript,
                    ShouldExcludeExpired = shouldExcludeExpired,
                    OperateOnTypes = operateOnTypes
                };

                var smuggler = new SmugglerApi(smugglerOptions, DatabaseCommands, message => Report(message));

                await smuggler.ImportData(stream, smugglerOptions);
            }

            return DatabaseTaskOutcome.Succesful;
        }
    }
}
