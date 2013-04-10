using System;
using System.Globalization;
using System.Windows.Data;

namespace Raven.Studio.Infrastructure.Converters
{
	public class BreakToLinesConverter : IValueConverter
	{
		public object Convert(object value, Type targetType, object parameter, CultureInfo culture)
		{
			var str = value as string;
			if (str != null)
			{
				var list = str.Split(new[] {", "}, StringSplitOptions.RemoveEmptyEntries);
				return string.Join(Environment.NewLine, list);
			}

			return null;
		}

		public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
		{
			throw new NotImplementedException();
		}
	}
}
