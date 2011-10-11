using System;
using System.Globalization;
using System.IO;
using System.Windows.Data;

namespace Raven.Studio.Infrastructure.Converters
{
	public class FirstLineOnlyConverter : IValueConverter
	{
		public object Convert(object value, Type targetType, object parameter, CultureInfo culture)
		{
			var str = value as string;
			if (str == null)
				return value;

			using (var reader = new StringReader(str))
			{
				var readLine = reader.ReadLine();
				var hasMoreLines = reader.ReadLine() != null;
				if (hasMoreLines)
				{
					readLine = readLine + " ...";
				}
				return readLine;
			}
		}

		public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
		{
			throw new NotImplementedException();
		}
	}
}