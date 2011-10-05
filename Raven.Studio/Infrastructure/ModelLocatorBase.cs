using Raven.Client.Connection.Async;
using Raven.Studio.Models;

namespace Raven.Studio.Infrastructure
{
	public abstract class ModelLocatorBase<T> where T : Model
	{
		protected ServerModel ServerModel { get; private set; }
		protected DatabaseModel DatabaseModel { get; private set; }

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

		    DatabaseModel = ServerModel.SelectedDatabase.Value;
			var asyncDatabaseCommands = DatabaseModel.AsyncDatabaseCommands;
			Load(asyncDatabaseCommands, observable);
		}


		protected abstract void Load(IAsyncDatabaseCommands asyncDatabaseCommands, Observable<T> observable);

		public static string GetParamAfter(string urlPrefix)
		{
			var url = ApplicationModel.Current.NavigationState;
			if (url.StartsWith(urlPrefix) == false)
				return null;

			return url.Substring(urlPrefix.Length);
		}
	}
}