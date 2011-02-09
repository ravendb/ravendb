namespace Raven.Studio.Converters
{
	using System;
	using System.Globalization;
	using System.Linq;
	using System.Windows.Data;
	using System.Windows.Media;
	using Abstractions.Data;
	using Caliburn.Micro;
	using Framework;

	public class MagnitudeToListConverter : IValueConverter
	{
		private const int MaximumNumberOfItems = 15;
		
		public object Convert(object value, Type targetType, object parameter, CultureInfo culture)
		{
			var item = value as Collection;
			if (item == null)
			{
				return new[] { new SolidColorBrush(Colors.Blue), new SolidColorBrush(Colors.Green) };
			}
			var count = System.Convert.ToInt32(item.Count);
			var max = (parameter != null) ? System.Convert.ToInt32(parameter) : 100;
			var percent = (count*1.0 / max*1.0);
			var colors = IoC.Get<TemplateColorProvider>();
			var brush = new SolidColorBrush(colors.ColorFrom(item.Name));

			var top = (int)(percent * MaximumNumberOfItems);

			return Enumerable.Range(1, top).Select(x => brush);
		}

		public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
		{
			throw new NotImplementedException();
		}
	}
}