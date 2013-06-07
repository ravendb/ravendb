using System;
using System.Globalization;
using System.Windows;
using System.Windows.Data;
using Raven.Abstractions.Data;

namespace Raven.Studio.Infrastructure.Converters
{
	public class PriorityToFontStyleConverter : IValueConverter
	{
		public object Convert(object value, Type targetType, object parameter, CultureInfo culture)
		{
			var priority = value is IndexingPriority ? (IndexingPriority) value : IndexingPriority.Normal;

			return priority.HasFlag(IndexingPriority.Idle) ? FontStyles.Italic : FontStyles.Normal;
		}

		public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
		{
			throw new NotImplementedException();
		}
	}
}
