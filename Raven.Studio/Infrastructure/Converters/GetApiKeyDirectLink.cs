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
				var fullApiKey = value as string;
				if (fullApiKey == null)
					return "";
				if (fullApiKey.Contains("/") == false)
					return "Must set both name and secret to get a direct link";

				return string.Format(@"{0}/raven/studio.html#/databases?api-key={1}",
													 ApplicationModel.Current.Server.Value.Url, fullApiKey);
			}

			public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
			{
				throw new NotImplementedException();
			}
		}
}