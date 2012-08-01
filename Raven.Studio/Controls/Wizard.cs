using System.Collections.Generic;
using System.Threading.Tasks;
using System.Windows.Controls;

namespace Raven.Studio.Controls
{
	public class Wizard
	{
		private readonly List<ChildWindow> windows;
		TaskCompletionSource<List<ChildWindow>> tcs = new TaskCompletionSource<List<ChildWindow>>();
		private int location;

		public Wizard(List<ChildWindow> windows)
		{
			this.windows = windows;
			foreach (var childWindow in windows)
			{
				childWindow.Closed += (sender, args) =>
				{
					var window = sender as ChildWindow;
					if (window != null && window.DialogResult == true)
						ShowNext();
					else
						tcs.SetCanceled();
				};
			}
		}

		private void ShowNext()
		{
			if (location < windows.Count)
				windows[location++].Show();
			else
				tcs.SetResult(windows);
		}

		public Task<List<ChildWindow>> StartAsync()
		{
			ShowNext();

			return tcs.Task;
		}
	}
}
