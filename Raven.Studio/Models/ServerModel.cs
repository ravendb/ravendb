using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using System.Windows.Browser;
using Raven.Abstractions.Data;
using Raven.Client;
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Json.Linq;
using Raven.Studio.Commands;
using Raven.Studio.Impl;
using Raven.Studio.Infrastructure;
using Raven.Studio.Messages;
using Raven.Abstractions.Extensions;

namespace Raven.Studio.Models
{
	public class ServerModel : Model, IDisposable
	{
		public readonly string Url;
		private DocumentStore documentStore;
		private string[] defaultDatabase;

		private string buildNumber;
		private Observable<bool> isConnected;
		public UserInfo UserInfo { get; set; }
		private bool UserInfoSet = false;

		private string rawUrl;
		public string RawUrl
		{
			get { return rawUrl; }
			set
			{
				if (string.IsNullOrWhiteSpace(value))
					rawUrl = null;
				else
				{
					if (Url.EndsWith("/"))
						rawUrl = Url + value;
					else
						rawUrl = Url + "/" + value;
				}

				OnPropertyChanged(() => RawUrl);
			}
		}
		public DocumentConvention Conventions
		{
			get { return documentStore.Conventions; }
		}
		public string BuildNumber
		{
			get { return buildNumber; }
			private set { buildNumber = value; OnPropertyChanged(() => BuildNumber); }
		}

		public ServerModel()
			: this(DetermineUri())
		{
			RefreshRate = TimeSpan.FromMinutes(2);
		}

		private ServerModel(string url)
		{
			Url = url;
			Databases = new BindableCollection<string>(name => name);
			RecentDocuments = new Dictionary<string, QueueModel<string>>();
			SelectedDatabase = new Observable<DatabaseModel>();
			License = new Observable<LicensingStatus>();
			IsConnected = new Observable<bool> { Value = true };
			UserInfo = new UserInfo();
			Initialize();
		}

		private void Initialize()
		{
			if (DesignerProperties.IsInDesignTool)
				return;

			documentStore = new DocumentStore
			{
				Url = Url
			};

			var urlParser = new UrlParser(UrlUtil.Url);
			var apiKey = urlParser.GetQueryParam("api-key");
			if (string.IsNullOrEmpty(apiKey) == false)
				documentStore.ApiKey = apiKey;

			documentStore.Initialize();

			// We explicitly enable this for the Studio, we rely on SL to actually get us the credentials, and that 
			// already gives the user a clear warning about the dangers of sending passwords in the clear. I think that 
			// this is sufficient warning and we don't require an additional step, so we can disable this check safely.
			documentStore.JsonRequestFactory.
						  EnableBasicAuthenticationOverUnsecuredHttpEvenThoughPasswordsWouldBeSentOverTheWireInClearTextToBeStolenByHackers
				=
				true;

			UpdateUserInfo();

			documentStore.JsonRequestFactory.ConfigureRequest += (o, eventArgs) =>
			{
				if (onWebRequest != null)
					onWebRequest(eventArgs.Client);
			};

			defaultDatabase = new[] { Constants.SystemDatabase };
			Databases.Set(defaultDatabase);
			SetCurrentDatabase(new UrlParser(UrlUtil.Url));

			//DisplayRawUrl();
			DisplayBuildNumber();
			DisplayLicenseStatus();
			TimerTickedAsync();
		}

		private void UpdateUserInfo()
		{
			if (SelectedDatabase.Value == null)
				return;
			SelectedDatabase.Value
			                .AsyncDatabaseCommands
			                .CreateRequest(string.Format("/debug/user-info").NoCache(), "GET")
			                .ReadResponseJsonAsync()
			                .ContinueOnSuccessInTheUIThread(doc =>
			                {
				                UserInfoSet = true;
				                UserInfo = DocumentStore.Conventions.CreateSerializer()
				                                           .Deserialize<UserInfo>(new RavenJTokenReader(doc));
			                });
		}

		public bool CreateNewDatabase { get; set; }
		private static bool firstTick = true;

		public override Task TimerTickedAsync()
		{
			return documentStore.AsyncDatabaseCommands.GetDatabaseNamesAsync(1024)
				.ContinueOnSuccess(names =>
									{
										Databases.Match(defaultDatabase.Concat(names).ToArray());
										if (firstTick == false)
											return;

										firstTick = false;
										if (UserInfoSet == false)
											UpdateUserInfo();

										if (UserInfo != null && UserInfo.IsAdminGlobal)
										{
											ApplicationModel.Current.Server.Value.DocumentStore
											                .AsyncDatabaseCommands
											                .ForSystemDatabase()
											                .GetAsync("Raven/StudioConfig")
											                .ContinueOnSuccessInTheUIThread(doc =>
											                {
												                if (doc != null && doc.DataAsJson.ContainsKey("WarnWhenUsingSystemDatabase"))
												                {
													                if (doc.DataAsJson.Value<bool>("WarnWhenUsingSystemDatabase") == false)
														                UrlUtil.Navigate("/documents");
												                }
											                });
										}

										var url = new UrlParser(UrlUtil.Url);

										if (url.QueryParams.ContainsKey("database") == false && (names.Length == 0 || (names.Length == 1 && names[0] == Constants.SystemDatabase)))
											CreateNewDatabase = true;

										if (string.IsNullOrEmpty(Settings.Instance.SelectedDatabase))
											return;

										if (Settings.Instance.SelectedDatabase != null && names.Contains(Settings.Instance.SelectedDatabase))
										{
											if (url.QueryParams.ContainsKey("database") == false)
											{
												url.SetQueryParam("database", Settings.Instance.SelectedDatabase);
												SetCurrentDatabase(url);
											}

											if (string.IsNullOrWhiteSpace(url.Path))
												UrlUtil.Navigate(Settings.Instance.LastUrl);
										}
									})
				.Catch();
		}

		public IDocumentStore DocumentStore
		{
			get { return documentStore; }
		}

		public Observable<DatabaseModel> SelectedDatabase { get; private set; }
		public BindableCollection<string> Databases { get; private set; }

		public void Dispose()
		{
			documentStore.Dispose();
		}

		private static string DetermineUri()
		{
			if (DesignerProperties.IsInDesignTool)
				return string.Empty;

			if (HtmlPage.Document.DocumentUri.Scheme == "file")
			{
				if (HtmlPage.Document.DocumentUri.Query.Contains("fiddler"))
					return "http://localhost.fiddler:8080";
				return "http://localhost:8080";
			}

			var localPath = HtmlPage.Document.DocumentUri.LocalPath;
			var lastIndexOfRaven = localPath.LastIndexOf("/raven/", StringComparison.Ordinal);
			if (lastIndexOfRaven != -1)
				localPath = localPath.Substring(0, lastIndexOfRaven);

			return new UriBuilder(HtmlPage.Document.DocumentUri)
			{
				Path = localPath,
				Fragment = ""
			}.Uri.ToString();
		}

		public void SetCurrentDatabase(UrlParser urlParser)
		{
			var databaseName = urlParser.GetQueryParam("database");

			if (SelectedDatabase.Value != null
				&& (SelectedDatabase.Value.Name == databaseName || (SelectedDatabase.Value.Name == Constants.SystemDatabase && databaseName == null)))
				return;

			if (SelectedDatabase.Value != null)
				SelectedDatabase.Value.Dispose();

			SelectedDatabase.Value = databaseName == null
				? new DatabaseModel(Constants.SystemDatabase, documentStore) : new DatabaseModel(databaseName, documentStore);

			SelectedDatabase.Value.AsyncDatabaseCommands
				.EnsureSilverlightStartUpAsync()
				.ContinueOnSuccess(() =>
				{
					if (databaseName != null && databaseName != Constants.SystemDatabase)
						Settings.Instance.SelectedDatabase = databaseName;
				})
				.Catch(exception =>
				{
					var webException = exception.ExtractSingleInnerException() as WebException;
					if (webException == null)
						return false;

					var httpWebResponse = webException.Response as HttpWebResponse;

					if (httpWebResponse == null)
						return false;

					if (httpWebResponse.StatusCode != HttpStatusCode.ServiceUnavailable)
						return false;

					ApplicationModel.Current.Notifications.Add(new Notification("Database " + databaseName + " does not exist.", NotificationLevel.Error, webException));

					return true;
				});
		}

		private void DisplayBuildNumber()
		{
			SelectedDatabase.Value.AsyncDatabaseCommands.GetBuildNumberAsync()
				.ContinueOnSuccessInTheUIThread(x => BuildNumber = x.BuildVersion)
				.Catch();
		}

		private void DisplayLicenseStatus()
		{
			SelectedDatabase.Value.AsyncDatabaseCommands.GetLicenseStatusAsync()
				.ContinueOnSuccessInTheUIThread(x =>
				{
					License.Value = x;
					if (x.Error == false)
						return;
					new ShowLicensingStatusCommand().Execute(x);
				})
				.Catch();
		}

		//private void DisplayRawUrl()
		//{
		//	if (SelectedDatabase != null && SelectedDatabase.Value != null && SelectedDatabase.Value.Name != null)
		//		RawUrl = Path.Combine(Url, "databases", SelectedDatabase.Value.Name);
		//	else
		//		RawUrl = Url;
		//}

		public Dictionary<string, QueueModel<string>> RecentDocuments { get; set; }

		public Observable<LicensingStatus> License { get; private set; }
		public Observable<bool> IsConnected
		{
			get { return isConnected; }
			set
			{
				isConnected = value;
				OnPropertyChanged(() => IsConnected);
			}
		}

		public void SetConnected(bool isConnected)
		{
			IsConnected.Value = isConnected;
			OnPropertyChanged(() => IsConnected);
		}
	}
}
