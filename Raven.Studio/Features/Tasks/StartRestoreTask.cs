using System.Windows.Input;
using Raven.Studio.Commands;
using Raven.Studio.Models;

namespace Raven.Studio.Features.Tasks
{
	public class StartRestoreTask : TaskModel
	{
		public StartRestoreTask()
		{
			Name = "Restore Database";
			Description = "Restore a database.";
			IconResource = "Image_Restore_Tiny";
			TaskInputs.Add(new TaskInput("Location", @"C:\path-to-your-backup-folder"));
			TaskInputs.Add(new TaskInput("Database Name", ""));
		}

		public override ICommand Action
		{
			get { return new RestoreCommand(this); }
		}
	}
}