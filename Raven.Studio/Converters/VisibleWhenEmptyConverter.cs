namespace Raven.Studio.Converters
{
    using System;
    using System.Linq;
    using System.Collections;
    using System.Globalization;
    using System.Windows;
    using System.Windows.Data;

    public class VisibleWhenEmptyConverter : IValueConverter
    {

        public object Convert(object value, Type targetType, object parameter, CultureInfo culture)
        {
			var enumerable = value as IEnumerable;
			return (enumerable == null) || !enumerable.Cast<object>().Any()
				? Visibility.Visible
				: Visibility.Collapsed;	
        }

        public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
        {
            throw new NotImplementedException();
        }
    }
}