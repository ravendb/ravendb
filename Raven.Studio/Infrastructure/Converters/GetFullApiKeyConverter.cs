using System;
using System.Globalization;
using System.Windows.Data;
using Raven.Abstractions.Data;

namespace Raven.Studio.Infrastructure.Converters
{
	public class GetFullApiKeyConverter : IValueConverter
	{
		public object Convert(object value, Type targetType, object parameter, CultureInfo culture)
		{
			var apiKey = value as ApiKeyDefinition;
			if (apiKey == null)
				return "";
			if (string.IsNullOrWhiteSpace(apiKey.Name) || string.IsNullOrWhiteSpace(apiKey.Secret))
				return "Must set both name and secret to get the full api key";
			return apiKey.Name + "/" + apiKey.Secret;
		}

		public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
		{
			throw new NotImplementedException();
		}
	}
}