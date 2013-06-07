using System.Linq;
using System.Windows.Input;
using Raven.Abstractions.Data;
using Raven.Json.Linq;
using Raven.Studio.Commands;
using Raven.Studio.Infrastructure;
using Raven.Studio.Models;

namespace Raven.Studio.Features.Tasks
{
	public class StartBackupTaskSectionModel : BasicTaskSectionModel<BackupDatabaseTask>
	{
		public BackupStatus Status { get; set; }

		public StartBackupTaskSectionModel()
		{
			Name = "Backup Database";
			Description = "Backup your database.";
		    IconResource = "Image_Backup_Tiny";
			TaskInputs.Add(new TaskInput("Location", @"C:\path-to-your-backup-folder"));
		}

        protected override BackupDatabaseTask CreateTask()
        {
            var location = TaskInputs.FirstOrDefault(x => x.Name == "Location");

            return new BackupDatabaseTask(DatabaseCommands, Database.Value.Name, location.Value as string);
        }
	}
}