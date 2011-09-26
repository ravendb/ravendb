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
		}

		public int GetSkipCount()
		{
			var queryParam = ApplicationModel.Current.GetQueryParam("skip");
			if (string.IsNullOrEmpty(queryParam))
				return 0;
			int result;
			int.TryParse(queryParam, out result);
			return result;
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