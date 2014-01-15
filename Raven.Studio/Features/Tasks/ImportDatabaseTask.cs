using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using System.Windows.Controls;
using Raven.Abstractions.Smuggler;
using Raven.Client.Connection.Async;
using Raven.Studio.Features.Input;
using Raven.Studio.Features.Smuggler;
using Raven.Studio.Models;

namespace Raven.Studio.Features.Tasks
{
    public class ImportDatabaseTask : DatabaseTask
    {
        private readonly int batchSize;
        private readonly List<FilterSetting> filterSettings;
        private readonly bool includeAttachements;
        private readonly bool includeDocuments;
        private readonly bool includeIndexes;
        private readonly bool includeTransformers;
        private readonly bool removeAnalyzers;
        private readonly bool shouldExcludeExpired;
        private readonly string transformScript;

        public ImportDatabaseTask(IAsyncDatabaseCommands databaseCommands, string databaseName, bool includeAttachements,
            bool includeDocuments, bool includeIndexes,
            bool removeAnalyzers,
            bool includeTransformers, bool shouldExcludeExpired, int batchSize, string transformScript,
            List<FilterSetting> filterSettings)
            : base(databaseCommands, "Import Database", databaseName)
        {
            this.includeAttachements = includeAttachements;
            this.includeDocuments = includeDocuments;
            this.includeIndexes = includeIndexes;
            this.removeAnalyzers = removeAnalyzers;
            this.includeTransformers = includeTransformers;
            this.shouldExcludeExpired = shouldExcludeExpired;
            this.batchSize = batchSize;
            this.transformScript = transformScript;
            this.filterSettings = filterSettings;
        }

        protected override async Task<DatabaseTaskOutcome> RunImplementation()
        {
            if (ApplicationModel.Current.Server.Value.SelectedDatabase.Value.Statistics.Value != null
                && ApplicationModel.Current.Server.Value.SelectedDatabase.Value.Statistics.Value.CountOfDocuments != 0)
            {
                return
                    await
                        AskUser.ConfirmationWithContinuation("Override Documents?",
                            "There are documents in the database :" +
                            ApplicationModel.Current.Server.Value.SelectedDatabase.Value.Name +
                            "." + Environment.NewLine
                            + "This operation can override those documents.", ExecuteInternal,
                            () => TaskEx.FromResult(DatabaseTaskOutcome.Abandoned));
            }
            return await ExecuteInternal();
        }

        private async Task<DatabaseTaskOutcome> ExecuteInternal()
        {
            if (includeDocuments == false && includeAttachements == false && includeIndexes == false &&
                includeTransformers == false)
                return DatabaseTaskOutcome.Abandoned;

            var openFile = new OpenFileDialog
            {
                Filter = "Raven Dumps|*.ravendump;*.raven.dump",
            };

            if (openFile.ShowDialog() != true)
                return DatabaseTaskOutcome.Abandoned;

            Report(String.Format("Importing from {0}", openFile.File.Name));

            using (FileStream stream = openFile.File.OpenRead())
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

                if (removeAnalyzers)
                {
                    operateOnTypes |= ItemType.RemoveAnalyzers;
                }

                if (includeTransformers)
                {
                    operateOnTypes |= ItemType.Transformers;
                }

                var smuggler = new SmugglerApi(DatabaseCommands, message => Report(message));
				await smuggler.ImportData(new SmugglerImportOptions { FromStream = stream }, new SmugglerOptions
                {
                    BatchSize = batchSize,
                    Filters = filterSettings,
                    TransformScript = transformScript,
                    ShouldExcludeExpired = shouldExcludeExpired,
                    OperateOnTypes = operateOnTypes,
                });
            }

            return DatabaseTaskOutcome.Succesful;
        }

        public override void OnError()
        {
        }
    }
}