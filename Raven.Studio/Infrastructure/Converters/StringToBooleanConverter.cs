using System;
using System.Globalization;
using System.Windows.Data;

namespace Raven.Studio.Infrastructure.Converters
{
	public class StringToBooleanConverter : IValueConverter
	{
		public object Convert(object value, Type targetType, object parameter, CultureInfo culture)
		{
			var data = value as string;
			return data != null && !string.IsNullOrWhiteSpace(data);
		}

		public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
		{
			throw new NotImplementedException();
		}
	}
}
