using Raven.Studio.Infrastructure;
using Raven.Studio.Models;

namespace Raven.Studio.Commands
{
	public class ChangeDatabaseCommand : Command
	{
		private string databaseName;

		public override bool CanExecute(object parameter)
		{
			databaseName = parameter as string;
			return string.IsNullOrEmpty(databaseName) == false && ApplicationModel.Current.Server.Value != null;
		}

		public override void Execute(object parameter)
		{
			bool shouldRedirect = true;

			var urlParser = new UrlParser(UrlUtil.Url);
			if (urlParser.GetQueryParam("database") == databaseName)
				shouldRedirect = false;

			urlParser.SetQueryParam("database", databaseName);

			var server = ApplicationModel.Current.Server.Value;
			server.SetCurrentDatabase(urlParser);
			server.SelectedDatabase.Value.AsyncDatabaseCommands
				.EnsureSilverlightStartUpAsync()
				.Catch();

			View.UpdateAllFromServer();
			new HomeModel().ForceTimerTicked();
			new CollectionsModel().ForceTimerTicked();
			new IndexesModel().ForceTimerTicked();
			new AllDocumentsModel().ForceTimerTicked();

			if (shouldRedirect)
			{
				UrlUtil.Navigate(urlParser.BuildUrl());
			}
		}
	}
}