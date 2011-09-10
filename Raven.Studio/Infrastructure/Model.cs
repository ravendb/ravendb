using System.Threading.Tasks;

namespace Raven.Studio.Infrastructure
{
	public class Model : NotifyPropertyChangedBase
	{
		private Task currentTask;

		internal virtual void TimerTicked()
		{
			if (currentTask != null)
				return;

			var task = TimerTickedAsync();

			if (task == null)
				return;

			task.Finally(() => currentTask = null)
				.Catch();
		}

		protected virtual Task TimerTickedAsync()
		{
			return null;
		}
	}
}