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

			errorColorBrush = GetBrushFromHexString("FFFFC0CB");
			debugColorBrush = GetBrushFromHexString("FFFFEFD5");
			infoColorBrush = GetBrushFromHexString("FFE0FFFF");
			defaultColorBrush = new SolidColorBrush(Colors.White);

			LogsList.LoadingRow += LogsList_LoadingRow;
		}

		private void LogsList_LoadingRow(object sender, DataGridRowEventArgs e)
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

		public static SolidColorBrush GetBrushFromHexString(string aarrggbb)
		{
			String xamlString = "<Canvas xmlns=\"http://schemas.microsoft.com/winfx/2006/xaml/presentation\" Background=\"#" + aarrggbb + "\"/>";
			var c = (Canvas)System.Windows.Markup.XamlReader.Load(xamlString);
			return (SolidColorBrush)c.Background;
		}
	}
}