using System;
using System.Reflection;
using System.Windows;
using Raven.Client.Connection.Async;
using Raven.Studio.Infrastructure;
using Raven.Studio.Messages;
using System.Linq;

namespace Raven.Studio.Models
{
    public class ApplicationModel : NotifyPropertyChangedBase
	{
		public static ApplicationModel Current { get; private set; }

		static ApplicationModel()
		{
			Current = new ApplicationModel();
		}

		private ApplicationModel()
		{
			Notifications = new BindableCollection<Notification>(x=>x.Message);
		    Notifications.CollectionChanged += delegate { OnPropertyChanged(() => ErrorCount); };
			LastNotification = new Observable<string>();
			Server = new Observable<ServerModel> {Value = new ServerModel()};
		    State = new ApplicationState();
		}

	    public ApplicationState State { get; private set; }

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
			                		if (Notifications.Count > 10)
			                		{
			                			Notifications.RemoveAt(0);
			                		}
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
            AddNotification(new Notification(message ?? exception.Message, NotificationLevel.Error, exception, details));
        }

		public Observable<string> LastNotification { get; set; }

		public BindableCollection<Notification> Notifications { get; set; }


        public int ErrorCount {get { return Notifications.Count(n => n.Level == NotificationLevel.Error); }}

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
			var firstOrDefault = (AssemblyFileVersionAttribute)typeof(ApplicationModel).Assembly.GetCustomAttributes(typeof(AssemblyFileVersionAttribute), true).FirstOrDefault();
			if (firstOrDefault != null)
				return firstOrDefault.Version;

			return "0.0.unknown.0";
		}
	}
}
