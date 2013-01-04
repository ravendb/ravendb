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
					{
						var asyncDatabaseCommands = ApplicationModel.Current.Server.Value.DocumentStore
							.AsyncDatabaseCommands
							.ForDefaultDatabase();
						var relativeUrl = "/admin/databases/" + databasesModel.SelectedDatabase.Name;
						if (deleteDatabase.hardDelete.IsChecked == true)
							relativeUrl += "?hard-delete=true";
						var httpJsonRequest = asyncDatabaseCommands.CreateRequest(relativeUrl, "DELETE");
						httpJsonRequest.ExecuteRequestAsync()
							.ContinueOnSuccessInTheUIThread(() =>
							{
								var database =
									ApplicationModel.Current.Server.Value.Databases.FirstOrDefault(
										s => s != Constants.SystemDatabase && s != databasesModel.SelectedDatabase.Name) ??
									Constants.SystemDatabase;
								ExecuteCommand(new ChangeDatabaseCommand(), database);
							});
					}
				});
		}
	}
}