using System;
using System.ComponentModel;
using System.Reactive.Linq;
using System.Windows.Media;
using Raven.Studio.Infrastructure;
using Raven.Studio.Models;

namespace Raven.Studio.Features.Databases
{
	public partial class LicenseView : View
	{
		public LicenseView()
		{
			InitializeComponent();

			Loaded += (sender, args) => ApplicationModel.Current.Server.Value.License.PropertyChanged += ShouldStartAlert;
		}

		private void ShouldStartAlert(object sender, PropertyChangedEventArgs e)
		{
			var licensing = ApplicationModel.Current.Server.Value.License.Value;
			if (licensing.Error)
			{
				StartAlert();
			}
		}

		private void StartAlert()
		{
			Brush originalBrsush = StatusText.Foreground;
			Brush alertBrush = new SolidColorBrush(Colors.Red);

			Observable.Timer(TimeSpan.Zero, TimeSpan.FromSeconds(2))
				.ObserveOnDispatcher()
				.Subscribe(_ => StatusText.Foreground = alertBrush);

			Observable.Timer(TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(2))
				.ObserveOnDispatcher()
				.Subscribe(_ => StatusText.Foreground = originalBrsush);
		}
	}
}