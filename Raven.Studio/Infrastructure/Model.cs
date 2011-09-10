using System;
using System.Threading.Tasks;

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
		}

		internal void ForceTimerTicked()
		{
			lastRefresh = DateTime.MinValue;
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
	}
}