using System.Windows;
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
			Current.Initialize();
		}

		private ApplicationModel()
		{
			Notifications = new BindableCollection<Notification>(x=>x.Message);
			LastNotification = new Observable<string>();
			Server = new Observable<ServerModel>();
		}

		private void Initialize()
		{
			var serverModel = new ServerModel();
			Server.Value = serverModel;
			serverModel.Initialize();
		}

		public Observable<ServerModel> Server { get; set; }

		public void Setup(FrameworkElement rootVisual)
		{
			rootVisual.DataContext = this;
		}

		public void AddNotification(Notification notification)
		{
			Notifications.Execute(() =>
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
	}
}