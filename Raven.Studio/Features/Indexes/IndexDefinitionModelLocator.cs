using System;
using Raven.Studio.Infrastructure;
using Raven.Studio.Models;

namespace Raven.Studio.Features.Indexes
{
	public class IndexDefinitionModelLocator
	{
		public Observable<IndexDefinitionModel> Current
		{
			get
			{
				var observable = new Observable<IndexDefinitionModel>();
				LoadIndex(observable);
				return observable;
			}
		}

		private void LoadIndex(Observable<IndexDefinitionModel> observable)
		{
			var serverModel = ApplicationModel.Current.Server.Value;
			if (serverModel == null)
			{
				ApplicationModel.Current.Server.RegisterOnce(() => LoadIndex(observable));
				return;
			}
			
			ApplicationModel.Current.RegisterOnceForNavigation(() => LoadIndex(observable));

			var asyncDatabaseCommands = serverModel.SelectedDatabase.Value.AsyncDatabaseCommands;
			var name = ApplicationModel.Current.GetQueryParam("name");
			if (name == null)
				return;

			asyncDatabaseCommands.GetIndexAsync(name)
				.ContinueOnSuccess(index =>
				                   {
				                   	if (index == null)
				                   	{
				                   		ApplicationModel.Current.Navigate(new Uri("/DocumentNotFound?id=" + name, UriKind.Relative));
				                   		return;
				                   	}
				                   	observable.Value = new IndexDefinitionModel(index, asyncDatabaseCommands);
				                   }
				)
				.Catch();
		}
	}
}