using System;
using System.Globalization;
using System.Windows.Data;
using System.Windows.Media;

namespace Raven.Studio.Infrastructure.Converters
{
	public class RedIfError : IValueConverter
	{
		public object Convert(object value, Type targetType, object parameter, CultureInfo culture)
		{
			var text = value as string;
			if (text == null)
				return new SolidColorBrush(Colors.Black);

			if (text.Contains("Error") || text.Contains("Exception"))
				return new SolidColorBrush(Colors.Red);
			return new SolidColorBrush(Colors.Black);
		}

		public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
		{
			throw new NotImplementedException();
		}
	}
}
