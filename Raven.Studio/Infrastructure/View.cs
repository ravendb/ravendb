using System;
using System.Threading;
using System.Threading.Tasks;
using System.Windows.Controls;

namespace Raven.Studio.Infrastructure
{
	public abstract class View : Page
	{
		public static View CurrentView { get; set; }

		private static Timer _timer = new Timer(TimerCallback, null, TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(5));

		private static void TimerCallback(object state)
		{
			var currentView = CurrentView;
			if (currentView == null)
				return;
			var model = currentView.DataContext as Model;
			if(model == null)
				return;

			model.TimerTicked();
		}

		protected override void OnNavigatedTo(System.Windows.Navigation.NavigationEventArgs e)
		{
			CurrentView = this;
		}

		protected override void OnNavigatedFrom(System.Windows.Navigation.NavigationEventArgs e)
		{
			CurrentView = null;
		}
	}
}