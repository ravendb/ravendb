using System;
using System.Diagnostics;
using System.Linq;
using System.Windows;
using System.Windows.Interop;
using Raven.Studio.Infrastructure;
using Raven.Studio.Messages;

namespace Raven.Studio.Models
{
	public class ApplicationModel
	{
		public static ApplicationModel Current { get; private set; }
		private static string threadSafeNavigationState;

		static ApplicationModel()
		{
			Current = new ApplicationModel();
			Current.Initialize();
			threadSafeNavigationState = Application.Current.Host.NavigationState;
			Application.Current.Host.NavigationStateChanged += (sender, args) =>
			{
				threadSafeNavigationState = args.NewNavigationState;
			};
		}

		public string NavigationState { get { return threadSafeNavigationState; } }

		private ApplicationModel()
		{
			Notifications = new BindableCollection<Notification>(new PrimaryKeyComparer<Notification>(x=>x.Message));
			LastNotification = new Observable<string>();
			Server = new Observable<ServerModel>();
		}

		private void Initialize()
		{
			var serverModel = new ServerModel();
			serverModel.Initialize()
				.ContinueOnSuccess(() => Server.Value = serverModel);
		}

		public Observable<ServerModel> Server { get; set; }

		public void Setup(FrameworkElement rootVisual)
		{
			rootVisual.DataContext = this;
		}


		public void Navigate(Uri source)
		{
			if (Deployment.Current.Dispatcher.CheckAccess())
				Application.Current.Host.NavigationState = source.ToString();
			else
				Deployment.Current.Dispatcher.InvokeAsync(() => Application.Current.Host.NavigationState = source.ToString());
		}

		public static string GetQueryParam(string name)
		{
			var indexOf = threadSafeNavigationState.IndexOf('?');
			if (indexOf == -1)
				return null;

			var options = threadSafeNavigationState.Substring(indexOf + 1).Split(new[] { '&', }, StringSplitOptions.RemoveEmptyEntries);

			return (from option in options
					where option.StartsWith(name) && option.Length > name.Length && option[name.Length] == '='
					select option.Substring(name.Length + 1)
					).FirstOrDefault();
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