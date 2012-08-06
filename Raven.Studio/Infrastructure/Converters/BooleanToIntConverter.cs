using System;
using System.Globalization;
using System.Windows.Data;

namespace Raven.Studio.Infrastructure.Converters
{
	public class BooleanToIntConverter : IValueConverter
	{
		public object Convert(object value, Type targetType, object parameter, CultureInfo culture)
		{
			var boolvalue = value is bool && (bool) value;
			return boolvalue ? 1 : 0;
		}

		public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
		{
			var intvalue = value is int ? (int) value : 0;
			return intvalue != 0;
		}
	}
}