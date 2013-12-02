using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.IO;
using System.Net;
using System.Reflection;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Media.Imaging;
using Raven.Abstractions.Data;
using Raven.Client;
using Raven.Client.Changes;
using Raven.Client.Connection.Async;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;
using Raven.Studio.Features.Documents;
using Raven.Studio.Infrastructure;
using Raven.Studio.Messages;
using System.Linq;

namespace Raven.Studio.Models
{
	public class ApplicationModel : NotifyPropertyChangedBase
	{
	    private DocumentPadModel documentPadModel;
	    public static ApplicationModel Current { get; private set; }

		static ApplicationModel()
		{
			ChangesToDispose = new List<IDatabaseChanges>();
			Current = new ApplicationModel();
		}

		private ApplicationModel()
		{
			Notifications = new BindableCollection<Notification>(x => x.Message);
			Notifications.CollectionChanged += delegate
			{
				OnPropertyChanged(() => ErrorCount);
			};

			LastNotification = new Observable<string>();
			Server = new Observable<ServerModel> { Value = new ServerModel() };

			Alerts = new ObservableCollection<Alert>();

			Server.Value.SelectedDatabase.PropertyChanged += (sender, args) =>
			{
				Server.Value.SelectedDatabase.Value.Update();
				RegisterToAlerts();
			};

			RegisterToAlerts();
			State = new ApplicationState();
		}

		private void RegisterToAlerts()
		{
			Server.Value.SelectedDatabase.Value.Changes()
			      .ForDocument(Constants.RavenAlerts)
			      .Subscribe(notification => UpdateAlerts());
			UpdateAlerts();
		}

		public Settings Settings
		{
			get { return Settings.Instance; }
		}

		private void UpdateAlerts()
		{
		    Server.Value.DocumentStore.OpenAsyncSession(Server.Value.SelectedDatabase.Value.Name)
		          .LoadAsync<AlertsDocument>(Constants.RavenAlerts)
		          .ContinueOnSuccessInTheUIThread(alertsDoc =>
		          {
		              Alerts.Clear();
		              if (alertsDoc != null)
		              {
		                  foreach (var alert in alertsDoc.Alerts)
		                  {
		                      Alerts.Add(alert);
		                  }
		              }

		              OnPropertyChanged(() => Alerts);
		              OnPropertyChanged(() => UnOnserverdAlerts);
		              OnPropertyChanged(() => AlertsTitle);
		          });
		}

		public int UnOnserverdAlerts { get { return Alerts.Count(alert => alert.Observed == false); } }
		public string AlertsTitle
		{
			get
			{
				if (UnOnserverdAlerts == 0)
					return "Alerts";
				return "Alerts (" + UnOnserverdAlerts + ")";
			}
		}

		public ApplicationState State { get; private set; }

		public static Observable<DatabaseModel> Database { get { return Current.Server.Value.SelectedDatabase; } }

		public static IAsyncDatabaseCommands DatabaseCommands
		{
			get { return Database.Value.AsyncDatabaseCommands; }
		}

		public static JsonSerializer CreateSerializer()
		{
			return Current.Server.Value.DocumentStore.Conventions.CreateSerializer();
		}

		public Observable<ServerModel> Server { get; set; }

		public void Setup(FrameworkElement rootVisual)
		{
			rootVisual.DataContext = this;
		}

        public DocumentPadModel DocumentPad { get { return documentPadModel ?? (documentPadModel = new DocumentPadModel()); } }

        public void ShowDocumentInDocumentPad(string documentId)
        {
            DocumentPad.IsOpen = true;
            DocumentPad.DocumentId = documentId;
            DocumentPad.LoadDocument.Execute(null);
        }

		public void AddNotification(Notification notification)
		{
			Execute.OnTheUI(() =>
			{
				var originalNotification =
					Notifications.FirstOrDefault(notification1 => notification1.Message == notification.Message
					                                              && notification1.Details == notification.Details
					                                              && notification1.Level == notification.Level);
				if (originalNotification != null)
				{
					notification.RepeatCount = originalNotification.RepeatCount + 1;
					Notifications.Remove(originalNotification);
				}

				Notifications.Add(notification);
				if (Notifications.Count > 10)
					Notifications.RemoveAt(0);

				LastNotification.Value = notification.Message;
			});
		}

		public void AddInfoNotification(string message)
		{
			AddNotification(new Notification(message, NotificationLevel.Info));
		}

		public void AddWarningNotification(string message)
		{
			AddNotification(new Notification(message, NotificationLevel.Warning));
		}

		public void AddErrorNotification(Exception exception, string message = null, params object[] details)
		{
			if (message == null)
			{
				var webException = exception as WebException;
				if (webException != null)
				{
					var httpWebResponse = webException.Response as HttpWebResponse;
					if (httpWebResponse != null)
					{
						if (httpWebResponse.StatusCode == HttpStatusCode.Forbidden && webException.Data.Contains("Url"))
						{
							var url = webException.Data["Url"].ToString();
							if (string.IsNullOrWhiteSpace(url) == false && url.Contains("/admin/"))
							{
								message = "It seems you are trying to run an admin command but you are not an admin";
							}
						}
						if(message == null)
							message = (int)httpWebResponse.StatusCode + " " + httpWebResponse.StatusDescription;
						var stream = httpWebResponse.GetResponseStream();
						if (stream != null)
						{
							var objects = ExtractError(stream, httpWebResponse, details);

							details = objects.ToArray();
						}
					}
				}
				if (message == null)
					message = exception.Message;
			}

			AddNotification(new Notification(message, NotificationLevel.Error, exception, details));
		}

		public static List<object> ExtractError(Stream stream, HttpWebResponse httpWebResponse)
		{
			return ExtractError(stream, httpWebResponse, new List<object>());
		}

		public static List<object> ExtractError(Stream stream, HttpWebResponse httpWebResponse, IEnumerable<object> details)
		{
			var error = new StreamReader(stream).ReadToEnd();

			var objects = new List<object>(details);
			try
			{
				var item = RavenJObject.Parse(error);
				objects.Insert(0, "Server Error:");
				objects.Insert(1, "-----------------------------------------");
				objects.Insert(2, item.Value<string>("Url"));
				objects.Insert(3, item.Value<string>("Error"));
				objects.Insert(4, "-----------------------------------------");
				objects.Insert(5, Environment.NewLine);
				objects.Insert(6, Environment.NewLine);
			}
			catch (Exception)
			{
				objects.Insert(0, "Server sent:");
				objects.Insert(1, error);
				objects.Insert(2, Environment.NewLine);
				objects.Insert(3, Environment.NewLine);
			}

			if (httpWebResponse.StatusCode == HttpStatusCode.Unauthorized)
			{
				objects.Insert(0, "Could not get authorization for this command.");
				objects.Insert(1,
				               "If you should have access to this operation contact your admin and check the Raven/AnonymousAccess or the Windows Authentication settings in RavenDB ");
			}

			return objects;
		}

		public Observable<string> LastNotification { get; set; }

		public BindableCollection<Notification> Notifications { get; set; }

		public ObservableCollection<Alert> Alerts { get; set; }

		public int ErrorCount { get { return Notifications.Count(n => n.Level == NotificationLevel.Error); } }

		public string AssemblyVersion
		{
			get
			{
				var version = GetAssemblyVersion();

				return version.Split('.').LastOrDefault(x=>x != "0");
			}
		}
		public static List<IDatabaseChanges> ChangesToDispose { get; private set; }

		string GetAssemblyVersion()
		{
			var firstOrDefault = (AssemblyFileVersionAttribute)typeof(ApplicationModel).Assembly.GetCustomAttributes(typeof(AssemblyFileVersionAttribute), true).FirstOrDefault();
			if (firstOrDefault != null)
				return firstOrDefault.Version;

			return "0.0.unknown.0";
		}

		public void Refresh()
		{
			OnPropertyChanged(() => Settings);
		}
	}
}