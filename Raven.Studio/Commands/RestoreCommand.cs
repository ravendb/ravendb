using System.Linq;
using Raven.Studio.Features.Tasks;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Commands
{
	public class RestoreCommand : Command
	{
		private readonly StartRestoreTask startRestoreTask;

		public RestoreCommand(StartRestoreTask startRestoreTask)
		{
			this.startRestoreTask = startRestoreTask;
		}

		public override void Execute(object parameter)
		{
			var location = startRestoreTask.TaskInputs.FirstOrDefault(x => x.Name == "Location");
			var name = startRestoreTask.TaskInputs.FirstOrDefault(x => x.Name == "Database Name");

			if (location == null || name == null)
				return;

			DatabaseCommands.StartRestoreAsync(location.Value, name.Value)
				.ContinueWith(task => task.Wait()).Catch();
		}
	}
}
