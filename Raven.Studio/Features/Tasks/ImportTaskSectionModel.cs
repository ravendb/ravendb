using System.Linq;
using System.Windows.Input;
using Raven.Studio.Commands;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Features.Tasks
{
	public class ImportTaskSectionModel : SmugglerTaskSectionModel<ImportDatabaseTask>
	{
		public ImportTaskSectionModel()
		{
			Name = "Import Database";
            IconResource = "Image_Import_Tiny";
			Description = "Import data to the current database.\nImporting will overwrite any existing indexes.";
		}

        protected override ImportDatabaseTask CreateTask()
        {
            return new ImportDatabaseTask(
                DatabaseCommands,
                Database.Value.Name,
                includeAttachements: IncludeAttachments.Value,
                includeDocuments: IncludeDocuments.Value,
                includeIndexes: IncludeIndexes.Value,
                removeAnalyzers: this.RemoveAnalyzers.Value,
                includeTransformers: IncludeTransforms.Value,
                shouldExcludeExpired: Options.Value.ShouldExcludeExpired,
                batchSize: Options.Value.BatchSize,
                transformScript: Options.Value.TransformScript,
                filterSettings: GetFilterSettings()
                );
        }
	}
}