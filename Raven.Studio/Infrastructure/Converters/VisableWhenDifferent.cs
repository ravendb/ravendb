using System;
using System.Globalization;
using System.Windows;
using System.Windows.Data;

namespace Raven.Studio.Infrastructure.Converters
{
	public class VisableWhenDifferent : IValueConverter
	{
		public object Convert(object value, Type targetType, object parameter, CultureInfo culture)
		{
			if (value.Equals(parameter))
				return Visibility.Collapsed;
			return Visibility.Visible;
		}

		public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
		{
			throw new NotImplementedException();
		}
	}
}
