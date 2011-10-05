using Raven.Client.Connection.Async;
using Raven.Studio.Models;

namespace Raven.Studio.Infrastructure
{
	public abstract class ModelLocatorBase<T> where T : Model
	{
		protected ServerModel ServerModel { get; private set; }

		public Observable<T> Current
		{
			get
			{
				var observable = new Observable<T>();
				LoadModel(observable);
				return observable;
			}
		}

		private void LoadModel(Observable<T>  observable)
		{
			ServerModel = ApplicationModel.Current.Server.Value;
			if (ServerModel == null)
			{
				ApplicationModel.Current.Server.RegisterOnce(() => LoadModel(observable));
				return;
			}

		    var databaseModel = ServerModel.SelectedDatabase.Value;
		    var asyncDatabaseCommands = databaseModel.AsyncDatabaseCommands;
			Load(databaseModel, asyncDatabaseCommands, observable);
		}

		protected abstract void Load(DatabaseModel database, IAsyncDatabaseCommands asyncDatabaseCommands, Observable<T> observable);

		public static string GetParamAfter(string urlPrefix)
		{
			var url = ApplicationModel.Current.NavigationState;
			if (url.StartsWith(urlPrefix) == false)
				return null;

			return url.Substring(urlPrefix.Length);
		}
	}
}