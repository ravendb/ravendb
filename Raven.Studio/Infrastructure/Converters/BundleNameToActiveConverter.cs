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

			if (name == null || license == null || license.Attributes == null)
				return true;

			string active;
			if(license.Attributes.TryGetValue(name, out active) == false)
				return true;

			bool result;
			bool.TryParse(active, out result);
			return result;
		}

		public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
		{
			throw new NotImplementedException();
		}
	}
}
