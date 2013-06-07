using System;
using System.Globalization;
using System.Windows;
using System.Windows.Data;

namespace Raven.Studio.Infrastructure.Converters
{
	public class HiddenWhenLessOrEqualToConverter : IValueConverter
	{
		public object Convert(object value, Type targetType, object parameter, CultureInfo culture)
		{
		    try
		    {
		        return System.Convert.ToDouble(value) > System.Convert.ToDouble(parameter)
		                    ? Visibility.Visible
		                    : Visibility.Collapsed;
		    }
		    catch (InvalidCastException)
		    {
		        return Visibility.Visible;
		    }
		}

		public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
		{
			throw new NotImplementedException();
		}
	}
}