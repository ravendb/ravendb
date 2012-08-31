using Raven.Abstractions.Data;
using Raven.Studio.Features.Input;
using Raven.Studio.Infrastructure;
using Raven.Studio.Models;

namespace Raven.Studio.Commands
{
	public class SelectSystemDatabaseCommand : Command
	{
		public override bool CanExecute(object parameter)
		{
			if (parameter is DatabasesListModel)
				return true;
			return false;
		}
		public override void Execute(object parameter)
		{
			AskUser.ConfirmationAsync("Are you sure?", "Meddling with the system database could cause irreversible damage")
				.ContinueWith(task =>
				{
					if (!task.Result)
						return;
					Command.ExecuteCommand(new ChangeDatabaseCommand(true), Constants.SystemDatabase);
				});
		}
	}
}
