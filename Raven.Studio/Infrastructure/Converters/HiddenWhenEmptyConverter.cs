using System;
using System.Collections;
using System.Globalization;
using System.Windows;
using System.Windows.Data;

namespace Raven.Studio.Infrastructure.Converters
{
	public class HiddenWhenEmptyConverter : IValueConverter
	{
		public object Convert(object value, Type targetType, object parameter, CultureInfo culture)
		{
			if (value is int)
				return ((int) value) > 0 ? Visibility.Visible : Visibility.Collapsed;

			var items = value as IList;
			return (items == null) || items.Count == 0
			       	? Visibility.Collapsed
			       	: Visibility.Visible;
		}

		public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
		{
			throw new NotImplementedException();
		}
	}
}