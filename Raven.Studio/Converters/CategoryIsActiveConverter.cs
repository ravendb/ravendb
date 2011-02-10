namespace Raven.Studio.Converters
{
	using System;
	using System.Globalization;
	using System.Windows.Data;
	using Plugin;

	public class CategoryIsActiveConverter : IValueConverter
	{
		public object Convert(object value, Type targetType, object parameter, CultureInfo culture)
		{
			var screen = value as IRavenScreen;
			if (screen != null)
			{
				return screen.Section.ToString() == parameter as string;
			}
			return false;
		}

		public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
		{
			throw new NotImplementedException();
		}
	}
}