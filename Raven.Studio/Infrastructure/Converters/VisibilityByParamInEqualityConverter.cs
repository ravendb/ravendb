using System;
using System.Globalization;
using System.Windows;
using System.Windows.Data;

namespace Raven.Studio.Infrastructure.Converters
{
	public class VisibilityByParamInEqualityConverter : IValueConverter
	{
		public object Convert(object value, Type targetType, object parameter, CultureInfo culture)
		{
			return System.Convert.ToInt32(value) == System.Convert.ToInt32(parameter) ? Visibility.Collapsed : Visibility.Visible;
		}

		public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
		{
			throw new NotSupportedException();
		}
	}
}