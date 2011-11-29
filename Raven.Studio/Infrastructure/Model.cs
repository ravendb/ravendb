using System;
using System.Diagnostics;
using System.Net;
using System.Threading.Tasks;
using Raven.Abstractions.Extensions;
using Raven.Client.Silverlight.Connection;
using Raven.Studio.Models;

namespace Raven.Studio.Infrastructure
{
	public abstract class Model : NotifyPropertyChangedBase
	{
		private Task currentTask;
		private DateTime lastRefresh;
		protected bool IsForced;
		protected TimeSpan RefreshRate { get; set; }

		protected Model()
		{
			RefreshRate = TimeSpan.FromSeconds(5);
		}

		internal void ForceTimerTicked()
		{
			lastRefresh = DateTime.MinValue;
			IsForced = true;
			TimerTicked();
		}

		internal void TimerTicked()
		{
			if (currentTask != null)
				return;

			lock (this)
			{
				if (currentTask != null)
					return;

				if (DateTime.Now - lastRefresh < GetRefreshRate())
					return;

				using(ServerModel.OnWebRequest(request => request.Headers["Raven-Timer-Request"] = "true"))
					currentTask = TimerTickedAsync();

				if (currentTask == null)
					return;

				currentTask
					.Catch()
					.Finally(() =>
					{
						lastRefresh = DateTime.Now;
						IsForced = false;
						currentTask = null;
					});
			}
		}

		private TimeSpan GetRefreshRate()
		{
			//if (Debugger.IsAttached)
			//    return RefreshRate.Add(TimeSpan.FromMinutes(5));
			return RefreshRate;
		}

		protected virtual Task TimerTickedAsync()
		{
			return null;
		}
		
		[ThreadStatic] protected static Action<WebRequest> onWebRequest;

		public static IDisposable OnWebRequest(Action<WebRequest> action)
		{
			onWebRequest += action;
			return new DisposableAction(() => onWebRequest = null);
		}
		
	}
}