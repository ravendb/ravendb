using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Extensions;
using Raven.Studio.Infrastructure;
using Raven.Studio.Models;

namespace Raven.Studio.Commands
{
	public class ChangeDatabaseCommand : Command
	{
		private string databaseName;
		private readonly IList<Type> refreshStaticModels = new List<Type>
		                                                   	{
		                                                   		typeof (HomeModel),
		                                                   		typeof (CollectionsModel),
		                                                   		typeof (IndexesModel),
		                                                   		typeof (AllDocumentsModel),
		                                                   	};

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

			var updateAllFromServer = PageView.UpdateAllFromServer();
			refreshStaticModels
				.Except(updateAllFromServer.Select(x=>x.GetType()))
				.Select(model => (Model) Activator.CreateInstance(model))
				.ForEach(model => model.ForceTimerTicked());
			
			
			if (shouldRedirect)
			{
				UrlUtil.Navigate(urlParser.BuildUrl());
			}
		}
	}
}