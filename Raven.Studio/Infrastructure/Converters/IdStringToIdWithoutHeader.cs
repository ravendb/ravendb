using System;
using System.Globalization;
using System.Windows.Data;

namespace Raven.Studio.Infrastructure.Converters
{
	public class IdStringToIdWithoutHeader : IValueConverter
	{
		public object Convert(object value, Type targetType, object parameter, CultureInfo culture)
		{
			var str = value as string;
			var header = parameter as string;
			if (str == null || header == null)
				return str;

			if(str.StartsWith(header) == false)
				return str;

			return str.Substring(header.Length);
		}

		public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
		{
			var str = value as string;
			var header = parameter as string;
			if (str == null || header == null)
				return str;

			return header + str;
		}
	}
}
