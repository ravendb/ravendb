using System;
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

			if (value == null || license == null || license.Attributes == null || license.Attributes.ContainsKey("Raven/ActiveBundles") == false)
				return false;

			return license.Attributes["Raven/ActiveBundles"].Contains(name);
		}

		public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
		{
			throw new NotImplementedException();
		}
	}
}
