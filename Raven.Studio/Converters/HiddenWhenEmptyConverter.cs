namespace Raven.Studio.Converters
{
	using System;
	using System.Collections;
	using System.Globalization;
	using System.Windows;
	using System.Windows.Data;

	public class HiddenWhenEmptyConverter : IValueConverter
	{
		public object Convert(object value, Type targetType, object parameter, CultureInfo culture)
		{
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