using System;
using System.Linq;
using System.Threading.Tasks;
using System.Windows.Browser;
using Raven.Client.Document;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Models
{
	public class ServerModel : Model
	{
		private readonly DocumentStore documentStore;
		private DatabaseModel[] defaultDatabase;

		public ServerModel()
			: this(DetermineUri())
		{
			
		}

		public ServerModel(string url)
		{
			Databases = new BindableCollection<DatabaseModel>();
			SelectedDatabase = new Observable<DatabaseModel>();

			documentStore = new DocumentStore
			{
				Url = url
			};

			documentStore.Initialize();

			// We explicitly enable this for the Studio, we rely on SL to actually get us the credentials, and that 
			// already gives the user a clear warning about the dangers of sending passwords in the clear. I think that 
			// this is sufficent warning and we don't require an additional step, so we can disable this check safely.
			documentStore.JsonRequestFactory.
				EnableBasicAuthenticationOverUnsecureHttpEvenThoughPasswordsWouldBeSentOverTheWireInClearTextToBeStolenByHackers =
				false;
		}

		public Task Initialize()
		{
			defaultDatabase = new[] { new DatabaseModel("Default", documentStore.AsyncDatabaseCommands) };
			return documentStore.AsyncDatabaseCommands.EnsureSilverlightStartUpAsync()
				.ContinueOnSuccess((Action)LoadValuesAfterEnsuringWeCanAccessServer);
		}

		private void LoadValuesAfterEnsuringWeCanAccessServer()
		{
			Databases.Set(defaultDatabase);
			SelectedDatabase.Value = defaultDatabase[0];
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
	}
}