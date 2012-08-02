using System.Linq;
using Raven.Abstractions.Data;
using Raven.Studio.Controls;
using Raven.Studio.Infrastructure;
using Raven.Studio.Models;
using Raven.Studio.Features.Input;

namespace Raven.Studio.Commands
{
	public class DeleteDatabaseCommand : Command
	{
		private readonly DatabasesListModel databasesModel;

		public DeleteDatabaseCommand(DatabasesListModel databasesModel)
		{
			this.databasesModel = databasesModel;
		}

		public override void Execute(object parameter)
		{
			new DeleteDatabase().ShowAsync()
				.ContinueOnSuccessInTheUIThread(deleteDatabase => {
					if (deleteDatabase.DialogResult == true)
						ApplicationModel.Current.Server.Value.DocumentStore
							.AsyncDatabaseCommands
							.ForDefaultDatabase()
							.DeleteDocumentAsync("Raven/Databases/" + databasesModel.SelectedDatabase.Name)
							.ContinueOnSuccessInTheUIThread(() =>
							{
								var database =
									ApplicationModel.Current.Server.Value.Databases.FirstOrDefault(
										s => s != Constants.SystemDatabase && s != databasesModel.SelectedDatabase.Name) ??
									Constants.SystemDatabase;
								ExecuteCommand(new ChangeDatabaseCommand(), database);
							});
				});
		}
	}
}