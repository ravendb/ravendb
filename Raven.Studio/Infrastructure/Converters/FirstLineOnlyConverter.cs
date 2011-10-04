using System;
using System.Globalization;
using System.Windows.Data;

namespace Raven.Studio.Infrastructure.Converters
{
	public class FirstLineOnlyConverter : IValueConverter
	{
		public object Convert(object value, Type targetType, object parameter, CultureInfo culture)
		{
			var str = value as string;
			if (str == null)
				return value;

			int newLinePosition = str.IndexOf(Environment.NewLine[0]);
			if (newLinePosition == -1)
				return str;
			return str.Substring(0, newLinePosition);
		}

		public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
		{
			throw new NotImplementedException();
		}
	}
}