namespace Raven.Studio.Converters
{
	using System;
	using System.Globalization;
	using System.Windows.Data;

	public class BooleanToOppositeBooleanConverter : IValueConverter
	{
		public object Convert(object value, Type targetType, object parameter, CultureInfo culture)
		{
			return ! System.Convert.ToBoolean(value);
		}

		public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
		{
			throw new NotImplementedException();
		}
	}
}