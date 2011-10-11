using System;
using System.Windows.Controls;
using System.Windows.Media;
using Raven.Abstractions.Data;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Features.Logs
{
	public partial class LogsView : View
	{
		private readonly SolidColorBrush errorColorBrush;
		private readonly SolidColorBrush debugColorBrush;
		private readonly SolidColorBrush infoColorBrush;
		private readonly SolidColorBrush defaultColorBrush;

		public LogsView()
		{
			InitializeComponent();

			errorColorBrush = new SolidColorBrush(Color.FromArgb());
			debugColorBrush = new SolidColorBrush(Colors.Yellow);
			infoColorBrush = new SolidColorBrush(Colors.Green);
			defaultColorBrush = new SolidColorBrush(Colors.White);

			LogsList.LoadingRow += LogsList_LoadingRow;
		}

		private void   LogsList_LoadingRow(object sender, System.Windows.Controls.DataGridRowEventArgs e)
		{
			var log = (LogItem)e.Row.DataContext;
			switch (log.Name)
			{
				case "Warn":
					e.Row.Background = errorColorBrush;
					break;
				case "Debug":
					e.Row.Background = debugColorBrush;
					break;
				case "Info":
					e.Row.Background = infoColorBrush;
					break;
				default:
					e.Row.Background = defaultColorBrush;
					break;
			}
		}
	}
}
