namespace Raven.Studio.Converters
{
	using System;
	using System.Globalization;
	using System.Linq;
	using System.Windows;
	using System.Windows.Data;
	using System.Windows.Media;
	using Abstractions.Data;
	using Caliburn.Micro;
	using Framework;

	public class MagnitudeToListConverter : DependencyObject, IValueConverter
	{
		const int MaximumNumberOfItems = 10;

		public static DependencyProperty MaximumProperty = DependencyProperty.Register(
			"Maximum",
			typeof (int),
			typeof (MagnitudeToListConverter),
			new PropertyMetadata(100));

		public int Maximum
		{
			get { return (int) GetValue(MaximumProperty); }
			set { SetValue(MaximumProperty, value); }
		}

		public object Convert(object value, Type targetType, object parameter, CultureInfo culture)
		{
			var item = value as Collection;
			if (item == null)
			{
				return new[] {new SolidColorBrush(Colors.Blue), new SolidColorBrush(Colors.Green)};
			}
			var count = System.Convert.ToInt32(item.Count);
			var percent = (count * 1.0 / Maximum * 1.0);
			percent = Math.Max(0,percent);
			percent = Math.Min(1,percent);

			var colors = IoC.Get<TemplateColorProvider>();
			var brush = new SolidColorBrush(colors.ColorFrom(item.Name));

			var top = (int) (percent*MaximumNumberOfItems);
			top = Math.Min(top,count);
			if (percent != 0) top = Math.Max(top, 1);

			return Enumerable.Range(1, top).Select(x => brush);
		}

		public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
		{
			throw new NotImplementedException();
		}
	}
}