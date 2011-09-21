using System;
using System.Linq;
using System.Threading.Tasks;
using System.Windows.Browser;
using System.Windows.Input;
using Raven.Abstractions.Extensions;
using Raven.Client.Document;
using Raven.Studio.Features.Databases;
using Raven.Studio.Infrastructure;
using Raven.Studio.Messages;

namespace Raven.Studio.Models
{
	public class ServerModel : Model
	{
		private readonly DocumentStore documentStore;
		private DatabaseModel[] defaultDatabase;

		public ServerModel()
			: this(DetermineUri())
		{
			RefreshRate = TimeSpan.FromMinutes(2);

			EventsBus.Subscribe<DatabaseCreated>(created => ForceTimerTicked());
		}

		private ServerModel(string url)
		{
			Actions = new BindableCollection<string>();
			Databases = new BindableCollection<DatabaseModel>();
			SelectedDatabase = new Observable<DatabaseModel>();

			documentStore = new DocumentStore
			{
				Url = url
			};

			documentStore.Initialize();

			// We explicitly enable this for the Studio, we rely on SL to actually get us the credentials, and that 
			// already gives the user a clear warning about the dangers of sending passwords in the clear. I think that 
			// this is sufficient warning and we don't require an additional step, so we can disable this check safely.
			documentStore.JsonRequestFactory.
				EnableBasicAuthenticationOverUnsecureHttpEvenThoughPasswordsWouldBeSentOverTheWireInClearTextToBeStolenByHackers =
				false;
		}

		public Task Initialize()
		{
			defaultDatabase = new[] { new DatabaseModel("Default", documentStore.AsyncDatabaseCommands) };
			return documentStore.AsyncDatabaseCommands.EnsureSilverlightStartUpAsync()
				.ContinueOnSuccess(() =>
				{
					Databases.Set(defaultDatabase);
					SelectedDatabase.Value = defaultDatabase[0];
				})
				.Catch();
		}

		protected override Task TimerTickedAsync()
		{
			return documentStore.AsyncDatabaseCommands.GetDatabaseNamesAsync()
				.ContinueOnSuccess(names =>
				{
					var databaseModels = names.Select(db => new DatabaseModel(db, documentStore.AsyncDatabaseCommands.ForDatabase(db)));
					Databases.Match(defaultDatabase.Concat(databaseModels).ToArray());
				})
				.Catch();
		}

		public Observable<DatabaseModel> SelectedDatabase { get; set; } 
		public BindableCollection<DatabaseModel> Databases { get; set; }
		public BindableCollection<string> Actions { get; set; }

		public ICommand CreateNewDatabase
		{
			get
			{
				return new CreateDatabaseCommand(this, documentStore.AsyncDatabaseCommands);
			}
		}

		public void Dispose()
		{
			documentStore.Dispose();
		}

		private static string DetermineUri()
		{
			if (HtmlPage.Document.DocumentUri.Scheme == "file")
			{
				return "http://localhost:8080";
			}
			var localPath = HtmlPage.Document.DocumentUri.LocalPath;
			var lastIndexOfRaven = localPath.LastIndexOf("/raven/");
			if (lastIndexOfRaven != -1)
			{
				localPath = localPath.Substring(0, lastIndexOfRaven);
			}
			return new UriBuilder(HtmlPage.Document.DocumentUri)
			{
				Path = localPath
			}.Uri.ToString();
		}

		public IDisposable Starting(string action)
		{
			Actions.Execute(() => Actions.Add(action));
			return new DisposableAction(() => Actions.Execute(() => Actions.Remove(action)));
		}
	}
}