using System.Collections.Generic;
using System.Linq;
using System.Windows.Input;
using Raven.Abstractions.Smuggler;
using Raven.Studio.Commands;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Features.Tasks
{
	public class ExportTaskSectionModel : SmugglerTaskSectionModel<ExportDatabaseTask>
	{
		public ExportTaskSectionModel()
		{
			Name = "Export Database";
		    IconResource = "Image_Export_Tiny";
			Description = "Export your database to a dump file.";
		}

        protected override ExportDatabaseTask CreateTask()
        {
            return new ExportDatabaseTask(
                DatabaseCommands, 
                Database.Value.Name,
                includeAttachements: IncludeAttachments.Value,
                includeDocuments: IncludeDocuments.Value,
                includeIndexes: IncludeIndexes.Value,
                includeTransformers:IncludeTransforms.Value,
                removeAnalyzers: RemoveAnalyzers.Value,
                shouldExcludeExpired:Options.Value.ShouldExcludeExpired,
                batchSize:Options.Value.BatchSize,
                transformScript:Options.Value.TransformScript,
                filterSettings: GetFilterSettings()
                );
        }
	}
}