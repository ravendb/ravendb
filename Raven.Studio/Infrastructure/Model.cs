using System;
using System.Diagnostics;
using System.Threading.Tasks;

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
	}
}