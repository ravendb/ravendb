using System;
using System.Globalization;
using System.Windows.Data;
using Raven.Abstractions.Data;
using Raven.Studio.Models;
using System.Linq;

namespace Raven.Studio.Infrastructure.Converters
{
	public class GetFullConnectionStringConverter : IValueConverter
	{
		public object Convert(object value, Type targetType, object parameter, CultureInfo culture)
		{
			var apiKey = value as string;
			if (apiKey == null)
				return "Must set both name and secret to get the connection string";

			return string.Format(@"Url = {0}; {1}", ApplicationModel.Current.Server.Value.Url, apiKey);
		}

		public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
		{
			throw new NotImplementedException();
		}
	}
}
