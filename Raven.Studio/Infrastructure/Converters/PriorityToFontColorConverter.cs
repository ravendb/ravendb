using System;
using System.Globalization;
using System.Windows;
using System.Windows.Data;
using System.Windows.Media;
using Raven.Abstractions.Data;

namespace Raven.Studio.Infrastructure.Converters
{
	public class PriorityToFontColorConverter : IValueConverter
	{
		public object Convert(object value, Type targetType, object parameter, CultureInfo culture)
		{
			var priority = value is IndexingPriority ? (IndexingPriority)value : IndexingPriority.Normal;

			if(priority.HasFlag(IndexingPriority.Disabled))
			return new SolidColorBrush(Colors.Gray);

			if (priority.HasFlag(IndexingPriority.Abandoned))
				return new SolidColorBrush(Colors.LightGray);

			return new SolidColorBrush(Colors.Black);
		}

		public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
		{
			throw new NotImplementedException();
		}
	}
}
