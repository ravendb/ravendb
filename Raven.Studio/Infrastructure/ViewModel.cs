using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Connection.Async;
using Raven.Studio.Models;

namespace Raven.Studio.Infrastructure
{
	public abstract class ViewModel : Model
	{
		public List<string> ModelUrlIgnoreList { get; private set; }
		public string ModelUrl { get; set; }
		public bool IsLoaded { get; private set; }

		public ViewModel()
		{
			ModelUrlIgnoreList = new List<string>();
			Database = new Observable<DatabaseModel>();
			SetCurrentDatabase();
			ApplicationModel.Current.Server.Value.SelectedDatabase.PropertyChanged += (sender, args) => SetCurrentDatabase();
		}

		protected void SetCurrentDatabase()
		{
			ApplicationModel.Current.Server.Value.SetCurrentDatabase(new UrlParser(UrlUtil.Url));
			Database.Value = ApplicationModel.Current.Server.Value.SelectedDatabase.Value;
		}

		public void LoadModel(string state)
		{
			if (string.IsNullOrWhiteSpace(state) == false &&
				state.StartsWith(ModelUrl, StringComparison.InvariantCultureIgnoreCase) &&
				ModelUrlIgnoreList.Any(state.StartsWith) == false)
			{
				LoadModelParameters(state.Substring(ModelUrl.Length));
			}
			IsLoaded = true;
		}

		public virtual void LoadModelParameters(string parameters) { }

		protected override Task TimerTickedAsync()
		{
			return IsLoaded == false ? null : LoadedTimerTickedAsync();
		}

		protected virtual Task LoadedTimerTickedAsync()
		{
			return null;
		}

		public Observable<DatabaseModel> Database { get; private set; }

		public IAsyncDatabaseCommands DatabaseCommands
		{
			get { return Database.Value.AsyncDatabaseCommands; }
		}
	}
}