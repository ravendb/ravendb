using System;
using System.Globalization;
using System.Linq;
using System.Windows.Data;
using Raven.Abstractions.Data;
using Raven.Studio.Models;

namespace Raven.Studio.Infrastructure.Converters
{
	public class GetApiKeyDirectLink : IValueConverter
	{
			public object Convert(object value, Type targetType, object parameter, CultureInfo culture)
			{
				var apiKey = value as ApiKeyDefinition;
				if (apiKey == null)
					return "";
				if (string.IsNullOrWhiteSpace(apiKey.Name) || string.IsNullOrWhiteSpace(apiKey.Secret))
					return "Must set both name and secret to get the connection string";

				return string.Format(@"{0}/raven/studio.html#/databases?api-Key={1}/{2}",
													 ApplicationModel.Current.Server.Value.Url, apiKey.Name, apiKey.Secret);
			}

			public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
			{
				throw new NotImplementedException();
			}
		}
}