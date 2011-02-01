namespace Raven.Studio.Converters
{
	using System;
	using System.Globalization;
	using System.Windows.Data;

	public class BooleanToOppositeBooleanConverter : IValueConverter
	{
		public object Convert(object value, Type targetType, object parameter, CultureInfo culture)
		{
			if (targetType != typeof (bool))
			{
				throw new InvalidOperationException("The target must be a Nullable<boolean>");
			}
			else if (value == null)
			{
				return false;
			}
			else
			{
				return !(bool) value;
			}
		}

		public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
		{
			throw new NotImplementedException();
		}
	}
}