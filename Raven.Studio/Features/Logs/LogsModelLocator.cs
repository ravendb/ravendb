// -----------------------------------------------------------------------
//  <copyright file="LogsModelLocator.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Studio.Infrastructure;
using Raven.Studio.Models;

namespace Raven.Studio.Features.Logs
{
	public class LogsModelLocator
	{
		public Observable<LogsModel> Current
		{
			get
			{
				var observable = new Observable<LogsModel>();
				LoadModel(observable);
				return observable;
			}
		}

		private void LoadModel(Observable<LogsModel> observable)
		{
			var serverModel = ApplicationModel.Current.Server.Value;
			if (serverModel == null)
			{
				ApplicationModel.Current.Server.RegisterOnce(() => LoadModel(observable));
				return;
			}

			ApplicationModel.Current.RegisterOnceForNavigation(() => LoadModel(observable));

			var asyncDatabaseCommands = serverModel.SelectedDatabase.Value.AsyncDatabaseCommands;
			observable.Value = new LogsModel(asyncDatabaseCommands);
		} 
	}
}