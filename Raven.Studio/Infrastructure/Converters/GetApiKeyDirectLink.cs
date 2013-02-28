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

				var url = ApplicationModel.Current.Server.Value.Url;
				return string.Format(@"{0}{1}raven/studio.html#/home?api-key={2}",
													 url, 
													 url.EndsWith("/") ? "" : "/",
													 fullApiKey);
			}

			public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
			{
				throw new NotImplementedException();
			}
		}
}