using System;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Raven.Abstractions;
using Raven.Abstractions.Extensions;
using Raven.Studio.Commands;
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
			IsForced = true;
			TimerTicked();
		}

		internal void TimerTicked()
		{
			if (ApplicationModel.Current.Server.Value.CreateNewDatabase && ApplicationModel.Current.Server.Value.UserInfo != null &&
			    ApplicationModel.Current.Server.Value.UserInfo.IsAdminGlobal)
			{
				ApplicationModel.Current.Server.Value.CreateNewDatabase = false;
				ApplicationModel.Current.Server.Value.DocumentStore
				                .AsyncDatabaseCommands
				                .ForSystemDatabase()
				                .GetAsync("Raven/StudioConfig")
				                .ContinueOnSuccessInTheUIThread(doc =>
				                {
					                if (doc != null && doc.DataAsJson.ContainsKey("WarnWhenUsingSystemDatabase"))
					                {
						                if (doc.DataAsJson.Value<bool>("WarnWhenUsingSystemDatabase") == false)
							                return;
					                }
					                Command.ExecuteCommand(new CreateDatabaseCommand());
				                });
			}

			if (currentTask != null)
				return;

			lock (this)
			{
				if (currentTask != null)
					return;

				var timeFromLastRefresh = SystemTime.UtcNow - lastRefresh;
				var refreshRate = GetRefreshRate();
				if (timeFromLastRefresh < refreshRate)
					return;

				using(OnWebRequest(request => request.DefaultRequestHeaders.Add("Raven-Timer-Request", "true")))
					currentTask = TimerTickedAsync();

				if (currentTask == null)
					return;

				currentTask
					.Catch()
					.Finally(() =>
					{
						lastRefresh = SystemTime.UtcNow;
						IsForced = false;
						currentTask = null;
					});
			}
		}

		private TimeSpan GetRefreshRate()
		{
			if (IsForced)
				return TimeSpan.FromSeconds(0.9);
			/*if (Debugger.IsAttached)
				return RefreshRate.Add(TimeSpan.FromSeconds(60));*/
			return RefreshRate;
		}

		public virtual Task TimerTickedAsync()
		{
			return null;
		}

	    [ThreadStatic] 
		protected static Action<HttpClient> onWebRequest;

		public static IDisposable OnWebRequest(Action<HttpClient> action)
		{
			onWebRequest += action;
			return new DisposableAction(() => onWebRequest = null);
		}
	}
}