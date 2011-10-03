// -----------------------------------------------------------------------
//  <copyright file="CollectionsModelLocator.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using Raven.Studio.Infrastructure;
using Raven.Studio.Models;
using Raven.Client.Connection.Async;

namespace Raven.Studio.Features.Collections
{
	public class CollectionsModelLocator
	{
		public Observable<DatabaseCollectionsModel> Current
		{
			get
			{
				var observable = new Observable<DatabaseCollectionsModel>();
				LoadModel(observable);
				return observable;
			}
		}

		private void LoadModel(Observable<DatabaseCollectionsModel> observable)
		{
			var serverModel = ApplicationModel.Current.Server.Value;
			if (serverModel == null)
			{
				ApplicationModel.Current.Server.RegisterOnce(() => LoadModel(observable));
				return;
			}

			ApplicationModel.Current.RegisterOnceForNavigation(() => LoadModel(observable));

			var asyncDatabaseCommands = serverModel.SelectedDatabase.Value.AsyncDatabaseCommands;
			observable.Value = new DatabaseCollectionsModel(asyncDatabaseCommands);
		} 
	}
}