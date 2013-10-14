using System.Linq;
using System.Windows.Input;
using Raven.Studio.Commands;
using Raven.Studio.Models;

namespace Raven.Studio.Features.Tasks
{
	public class StartRestoreTaskSectionModel : BasicTaskSectionModel<RestoreDatabaseTask>
	{
		public StartRestoreTaskSectionModel()
		{
			Name = "Restore Database";
			Description = "Restore a database.";
			IconResource = "Image_Restore_Tiny";
			TaskInputs.Add(new TaskInput("Backup Location", @"C:\path-to-your-backup-folder"));
			TaskInputs.Add(new TaskInput("Database Location", ""));
			TaskInputs.Add(new TaskInput("Database Name", ""));
			TaskInputs.Add(new TaskCheckBox("Defrag", false));
		}


	    protected override RestoreDatabaseTask CreateTask()
	    {
            var backupLocation = TaskInputs.First(x => x.Name == "Backup Location").Value as string;
            var databaseLocation = TaskInputs.First(x => x.Name == "Database Location").Value as string;
            var name = TaskInputs.First(x => x.Name == "Database Name").Value as string;
            var defrag = (bool)TaskInputs.First(x => x.Name == "Defrag").Value;

            return new RestoreDatabaseTask(DatabaseCommands, name, backupLocation, databaseLocation, defrag);
	    }
	}
}