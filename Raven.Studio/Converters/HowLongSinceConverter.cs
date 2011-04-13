namespace Raven.Studio.Converters
{
	using System;
	using System.Globalization;
	using System.Windows.Data;
	using Framework.Extensions;

	public class HowLongSinceConverter : IValueConverter
	{
		public object Convert(object value, Type targetType, object parameter, CultureInfo culture)
		{
			var dt = System.Convert.ToDateTime(value);
			if (dt.Kind == DateTimeKind.Local)
				return dt.HowLongSince();
			return dt.ToLocalTime().HowLongSince();
		}

		public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
		{
			throw new NotImplementedException();
		}
	}
}