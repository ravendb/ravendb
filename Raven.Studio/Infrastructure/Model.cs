using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Studio.Models;

namespace Raven.Studio.Infrastructure
{
	public class Model : NotifyPropertyChangedBase
	{
		private Task currentTask;
		private DateTime lastRefresh;
		protected TimeSpan RefreshRate { get; set; }

		public Model()
		{
			RefreshRate = TimeSpan.FromSeconds(5);
			Notification = new Observable<string>();
		}

		internal void ForceTimerTicked()
		{
			lastRefresh = DateTime.MinValue;
			TimerTicked();
		}

		internal void TimerTicked()
		{
			HandleNotifications();

			if (currentTask != null)
				return;

			lock (this)
			{
				if (currentTask != null)
					return;

				if (DateTime.Now - lastRefresh < RefreshRate)
					return;

				currentTask = TimerTickedAsync();

				if (currentTask == null)
					return;

				currentTask
					.Catch()
					.Finally(() =>
					{
						lastRefresh = DateTime.Now;
						currentTask = null;
					});
			}
		}

		protected virtual Task TimerTickedAsync()
		{
			return null;
		}

		private void HandleNotifications()
		{
			if (string.IsNullOrEmpty(Notification.Value) == false)
			{
				Notification.Value = null;
				ApplicationModel.RemoveNotification(NotificationType);
			}

			var notificationTypes = SubscribeForNotifications();
			if (notificationTypes == null)
				return;
			foreach (var subscribeForNotification in notificationTypes)
			{
				var notification = ApplicationModel.GetNotification(subscribeForNotification);
				if (notification == null)
					continue;
				Notification.Value = notification;
				NotificationType = subscribeForNotification;
				break;
			}
		}

		public Observable<string> Notification { get; set; }
		public Type NotificationType { get; set; }

		public virtual IEnumerable<Type> SubscribeForNotifications()
		{
			return null;
		}
	}
}