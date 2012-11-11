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
			var apiKey = value as ApiKeyDefinition;
			if (apiKey == null)
				return "";
			if (string.IsNullOrWhiteSpace(apiKey.Name) || string.IsNullOrWhiteSpace(apiKey.Secret))
				return "Must set both name and secret to get the connection string";
			var access = apiKey.Databases.FirstOrDefault();
			string dbName = access == null ? "DbName" : access.TenantId;

			return string.Format(@"Url = {0}; ApiKey = {1}/{2}; Database = {3}",
			                                     ApplicationModel.Current.Server.Value.Url, apiKey.Name, apiKey.Secret, dbName);
		}

		public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
		{
			throw new NotImplementedException();
		}
	}
}
