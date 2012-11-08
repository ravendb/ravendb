using System;
using System.Globalization;
using System.Windows.Data;
using Raven.Abstractions.Data;

namespace Raven.Studio.Infrastructure.Converters
{
	public class BundleNameToToolTipCoverter : IValueConverter
	{
		public object Convert(object value, Type targetType, object parameter, CultureInfo culture)
		{
			var name = parameter as string;
			var license = value as LicensingStatus;

			if (name == null)
				return "";

			if (license == null || license.Attributes == null)
				return name + " Bundle";

			string active;
			if (license.Attributes.TryGetValue(name, out active) == false)
				return name + " Bundle";

			bool result;
			bool.TryParse(active, out result);

			if(result)
				return name + " Bundle";

			return name + " Bundles is not approved in you license";
		}

		public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
		{
			throw new NotImplementedException();
		}
	}
}
