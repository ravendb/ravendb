using System.Windows.Input;
using Raven.Studio.Commands;
using Raven.Studio.Infrastructure;
using Raven.Studio.Models;

namespace Raven.Studio.Features.Tasks
{
    public class CsvImportTaskSectionModel : BasicTaskSectionModel<CsvImportTask>
    {
        public CsvImportTaskSectionModel()
        {
            Name = "Csv Import";
            Description = "Import a csv file into a collection. Each column will be treated as a property.";
            IconResource = "Image_ImportCsv_Tiny";
        }

        protected override CsvImportTask CreateTask()
        {
            return new CsvImportTask(DatabaseCommands, Database.Value.Name);
        }
    }
}