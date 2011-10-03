using Raven.Studio.Infrastructure;
using Raven.Studio.Models;

namespace Raven.Studio.Features.Indexes
{
	public class IndexesModelLocator
	{
		public Observable<IndexesModel> Current
		{
			get
			{
				var observable = new Observable<IndexesModel>();
				LoadIndex(observable);
				return observable;
			}
		}

		private void LoadIndex(Observable<IndexesModel> observable)
		{
			var serverModel = ApplicationModel.Current.Server.Value;
			if (serverModel == null)
			{
				ApplicationModel.Current.Server.RegisterOnce(() => LoadIndex(observable));
				return;
			}
			
			ApplicationModel.Current.RegisterOnceForNavigation(() => LoadIndex(observable));

			var asyncDatabaseCommands = serverModel.SelectedDatabase.Value.AsyncDatabaseCommands;
			observable.Value = new IndexesModel(asyncDatabaseCommands);
		}
	}
}