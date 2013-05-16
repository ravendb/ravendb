using System;
using System.Collections.Generic;
using System.Globalization;
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
using Raven.Abstractions.Smuggler;
using Raven.Client.Connection.Async;
using Raven.Studio.Features.Smuggler;
using Raven.Studio.Infrastructure;
using Raven.Studio.Models;
using Path = System.IO.Path;
using TaskStatus = System.Threading.Tasks.TaskStatus;

namespace Raven.Studio.Features.Tasks
{
    public class ExportDatabaseTask : DatabaseTask
    {
        const int BatchSize = 512;

        private bool includeAttachements, includeDocuments, includeIndexes, includeTransformers;
        private readonly bool shouldExcludeExpired;
        private readonly int batchSize;
        private readonly string transformScript;
        private readonly List<FilterSetting> filterSettings;

        public ExportDatabaseTask(IAsyncDatabaseCommands databaseCommands, string databaseName, bool includeAttachements, bool includeDocuments, bool includeIndexes,
                                  bool includeTransformers, bool shouldExcludeExpired, int batchSize, string transformScript, List<FilterSetting> filterSettings) : base(databaseCommands, databaseName, "Export Database")
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

        protected override async Task<DatabaseTaskOutcome> RunImplementation()
        {
            if (includeDocuments == false && includeAttachements == false && includeIndexes == false && includeTransformers == false)
                return DatabaseTaskOutcome.Abandoned;

            var saveFile = new SaveFileDialog
            {
                DefaultExt = ".ravendump",
                Filter = "Raven Dumps|*.ravendump;*.raven.dump",
            };

            var name = ApplicationModel.Database.Value.Name;
            var normalizedName = new string(name.Select(ch => Path.GetInvalidPathChars().Contains(ch) ? '_' : ch).ToArray());
            var defaultFileName = string.Format("Dump of {0}, {1}", normalizedName, DateTimeOffset.Now.ToString("dd MMM yyyy HH-mm", CultureInfo.InvariantCulture));
            try
            {
                saveFile.DefaultFileName = defaultFileName;
            }
            catch { }

            if (saveFile.ShowDialog() != true)
            {
                return DatabaseTaskOutcome.Abandoned;
            }

            using (var stream = saveFile.OpenFile())
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

                var smuggler = new SmugglerApi(new SmugglerOptions
                {
                    BatchSize = batchSize
                },
                                               DatabaseCommands,
                                               message => Report(message));

                await smuggler.ExportData(stream, new SmugglerOptions
                {
                    BatchSize = batchSize,
                    Filters = filterSettings,
                    TransformScript = transformScript,
                    ShouldExcludeExpired = shouldExcludeExpired,
                OperateOnTypes = operateOnTypes
                }, false);
            }

            return DatabaseTaskOutcome.Succesful;
        }
    }
}
