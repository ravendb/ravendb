using System;
using System.Collections.Generic;
using System.IO.IsolatedStorage;
using System.Linq;
using Raven.Abstractions.Extensions;
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
            var server = ApplicationModel.Current.Server.Value;
            if (server.SelectedDatabase.Value != null && server.SelectedDatabase.Value.Name == databaseName)
            {
                return;
            }

			bool shouldRedirect = true;

			var urlParser = new UrlParser(UrlUtil.Url);
			if (urlParser.GetQueryParam("database") == databaseName)
				shouldRedirect = false;

			urlParser.SetQueryParam("database", databaseName);

			
			server.SetCurrentDatabase(urlParser);
			server.SelectedDatabase.Value.AsyncDatabaseCommands
				.EnsureSilverlightStartUpAsync()
				.Catch();

			Settings.Instance.SelectedDatabase = databaseName;

			if (shouldRedirect)
			{
				UrlUtil.Navigate(urlParser.BuildUrl());
			}
		}
	}
}