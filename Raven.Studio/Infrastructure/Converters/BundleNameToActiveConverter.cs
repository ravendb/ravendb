using System;
using System.Collections.Generic;
using System.Globalization;
using System.Windows.Data;
using Raven.Abstractions.Data;

namespace Raven.Studio.Infrastructure.Converters
{
	public class BundleNameToActiveConverter : IValueConverter
	{
		public object Convert(object value, Type targetType, object parameter, CultureInfo culture)
		{
			var name = parameter as string;
			var license = value as LicensingStatus;

			if (name == null || license == null || license.Attributes == null) //|| license.Attributes.ContainsKey(name) == false)
				return false;
			//TODO: remove local attributes
			var localAttributes = new Dictionary<string, string>(StringComparer.InvariantCultureIgnoreCase);
			foreach (var attribute in license.Attributes)
			{
				localAttributes.Add(attribute.Key, attribute.Value);
			}

			return localAttributes[name];
		}

		public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
		{
			throw new NotImplementedException();
		}
	}
}
