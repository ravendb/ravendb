using System;
using System.Collections.Generic;
using System.Linq;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Navigation;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Models
{
	public class ApplicationModel : Model
	{
		public static ApplicationModel Current { get; private set; }

		static ApplicationModel()
		{
			Current = new ApplicationModel();
		}

		private ApplicationModel()
		{
			Server = new Observable<ServerModel>();
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
			Application.Current.Host.NavigationState = source.ToString();
		}


		public string GetQueryParam(string name)
		{
			var indexOf = Application.Current.Host.NavigationState.IndexOf('?');
			if (indexOf == -1)
				return null;

			var options = Application.Current.Host.NavigationState.Substring(indexOf+1).Split(new[] { '&', }, StringSplitOptions.RemoveEmptyEntries);

			return (from option in options 
					where option.StartsWith(name) && option.Length > name.Length && option[name.Length] == '=' 
					select option.Substring(name.Length + 1)
					).FirstOrDefault();
		}

		private static Dictionary<Type, string> notifications = new Dictionary<Type, string>();

		public static void AddNotification(Type type, string message)
		{
			RemoveNotification(type);
			notifications.Add(type, message);
		}

		public static string GetNotification(Type type)
		{
			if (notifications.ContainsKey(type) == false)
				return null;
			return notifications[type];
		}

		public static void RemoveNotification(Type type)
		{
			if (type != null && notifications.ContainsKey(type))
				notifications.Remove(type);
		}
	}
}