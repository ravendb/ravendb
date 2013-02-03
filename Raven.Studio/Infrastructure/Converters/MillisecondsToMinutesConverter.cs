using System;
using System.Globalization;
using System.Windows.Data;

namespace Raven.Studio.Infrastructure.Converters
{
	public class MillisecondsToMinutesConverter : IValueConverter
	{
		public object Convert(object value, Type targetType, object parameter, CultureInfo culture)
		{
			var interval = (int) value;
			return (int) (TimeSpan.FromMilliseconds(interval).TotalMinutes);
		}

		public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
		{
			return (int)TimeSpan.FromMinutes((double) value).TotalMilliseconds;
		}
	}
}