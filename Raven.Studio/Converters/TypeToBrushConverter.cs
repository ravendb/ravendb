namespace Raven.Studio.Converters
{
	using System;
	using System.Globalization;
	using System.Windows.Data;
	using System.Windows.Media;
	using Caliburn.Micro;
	using Framework;

	public class TypeToBrushConverter : IValueConverter
	{
		public object Convert(object value, Type targetType, object parameter, CultureInfo culture)
		{
			var key = value.ToString();
			var colors = IoC.Get<TemplateColorProvider>();
			return new SolidColorBrush(colors.ColorFrom(key));
		}

		public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
		{
			throw new NotImplementedException();
		}
	}
}