using System;
using System.Globalization;
using System.Windows.Data;

namespace Raven.Studio.Infrastructure.Converters
{
	public class BundleNameToActiveConverter : IValueConverter
	{
		public object Convert(object value, Type targetType, object parameter, CultureInfo culture)
		{
			var name = value as string;

			if (value == null)
				return false;

			if (ConfigurationManager.AppSettings.ContainsKey("Raven/ActiveBundles") == false)
				return false;
			var bundles = ConfigurationManager.AppSettings["Raven/ActiveBundles"];

			return bundles.Contains(name);
		}

		public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
		{
			throw new NotImplementedException();
		}
	}
}
