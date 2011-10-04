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

			ApplicationModel.Current.RegisterOnceForNavigation(() => LoadModel(observable));

			var asyncDatabaseCommands = ServerModel.SelectedDatabase.Value.AsyncDatabaseCommands;
			Load(asyncDatabaseCommands, observable);
		}

		protected abstract void Load(IAsyncDatabaseCommands asyncDatabaseCommands, Observable<T> observable);
	}
}