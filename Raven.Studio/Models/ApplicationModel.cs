using System;
using System.Reflection;
using System.Windows;
using Raven.Client.Connection.Async;
using Raven.Studio.Infrastructure;
using Raven.Studio.Messages;

namespace Raven.Studio.Models
{
	public class ApplicationModel
	{
		public static ApplicationModel Current { get; private set; }

		static ApplicationModel()
		{
			Current = new ApplicationModel();
		}

		private ApplicationModel()
		{
			Notifications = new BindableCollection<Notification>(x=>x.Message);
			LastNotification = new Observable<string>();
			Server = new Observable<ServerModel> {Value = new ServerModel()};
		}

		public static Observable<DatabaseModel> Database { get { return Current.Server.Value.SelectedDatabase; } }

		public static IAsyncDatabaseCommands DatabaseCommands
		{
			get { return Database.Value.AsyncDatabaseCommands; }
		}

		public Observable<ServerModel> Server { get; set; }

		public void Setup(FrameworkElement rootVisual)
		{
			rootVisual.DataContext = this;
		}

		public void AddNotification(Notification notification)
		{
			Execute.OnTheUI(() =>
			                	{
			                		Notifications.Add(notification);
			                		if (Notifications.Count > 5)
			                		{
			                			Notifications.RemoveAt(0);
			                		}
			                		LastNotification.Value = notification.Message;
			                	});
		}

		public Observable<string> LastNotification { get; set; }

		public BindableCollection<Notification> Notifications { get; set; }


		public string AssemblyVersion
		{
			get
			{
				var version = GetAssemblyVersion();

				return version.Split('.')[2];
			}
		}
		string GetAssemblyVersion()
		{
			var assemblyName = new AssemblyName(Application.Current.GetType().Assembly.FullName);
			var v = assemblyName.Version;

			return v == null ? string.Empty : v.ToString();
		}
	}
}