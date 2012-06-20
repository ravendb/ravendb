using System;
using System.ComponentModel;
using System.Reactive.Linq;
using System.Windows.Media;
using Raven.Studio.Infrastructure;
using Raven.Studio.Models;

namespace Raven.Studio.Features.Databases
{
	public partial class LicenseView : PageView
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
			Brush[] alertBrushes = new[]
			{
				new SolidColorBrush(Colors.Magenta),
				new SolidColorBrush(Colors.Orange),
				new SolidColorBrush(0xFFEC1B24.ToColor()),
				new SolidColorBrush(Colors.Yellow),
				new SolidColorBrush(Colors.Cyan),
			};

			int count = 0;
			Observable.Timer(TimeSpan.Zero, TimeSpan.FromSeconds(2))
				.ObserveOnDispatcher()
				.Subscribe(_ => StatusText.Foreground = alertBrushes[count++ % alertBrushes.Length]);

			Observable.Timer(TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(2))
				.ObserveOnDispatcher()
				.Subscribe(_ => StatusText.Foreground = originalBrsush);
		}
	}
}