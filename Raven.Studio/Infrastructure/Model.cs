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

		internal virtual void TimerTicked()
		{
			if (currentTask != null)
				return;

			if (DateTime.Now - lastRefresh < RefreshRate)
				return;
			
			var task = TimerTickedAsync();

			if (task == null)
				return;

			task
				.Catch()
				.Finally(() =>
				{
					lastRefresh = DateTime.Now;
					currentTask = null;
				});
		}

		protected virtual Task TimerTickedAsync()
		{
			return null;
		}
	}
}