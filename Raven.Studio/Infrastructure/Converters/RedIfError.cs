using System;
using System.Globalization;
using System.Windows.Data;
using System.Windows.Media;
using Raven.Studio.Features.Tasks;

namespace Raven.Studio.Infrastructure.Converters
{
	public class RedIfError : IValueConverter
	{
		public object Convert(object value, Type targetType, object parameter, CultureInfo culture)
		{
			var output = value as DatabaseTaskOutput;
            if (output == null)
				return new SolidColorBrush(Colors.Black);

			if (output.OutputType == OutputType.Error)
				return new SolidColorBrush(Color.FromArgb(255, 196, 29, 29));
			return new SolidColorBrush(Colors.Black);
		}

		public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
		{
			throw new NotImplementedException();
		}
	}
}
