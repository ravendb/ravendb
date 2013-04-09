using System;
using System.Globalization;
using System.Windows.Controls;
using System.Windows.Data;

namespace Raven.Studio.Infrastructure.Converters
{
	public class HorizontalIfStringConverter : IValueConverter
	{
		public object Convert(object value, Type targetType, object parameter, CultureInfo culture)
		{
			return value.ToString() == "System.String" ? Orientation.Horizontal : Orientation.Vertical;
		}

		public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
		{
			throw new NotImplementedException();
		}
	}
}
