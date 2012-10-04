using System;
using System.Globalization;
using System.Linq;
using System.Windows;
using System.Windows.Data;
using System.Windows.Media;
using Raven.Studio.Features.Documents;
using Raven.Studio.Models;

namespace Raven.Studio.Infrastructure.Converters
{
	public class MagnitudeToListConverter : DependencyObject, IValueConverter
	{
		const int MaximumNumberOfItems = 10;

		public static DependencyProperty MaximumProperty = DependencyProperty.Register(
			"Maximum",
			typeof(int),
			typeof(MagnitudeToListConverter),
			new PropertyMetadata(100));

		public int Maximum
		{
			get { return (int)GetValue(MaximumProperty); }
			set { SetValue(MaximumProperty, value); }
		}

		public object Convert(object value, Type targetType, object parameter, CultureInfo culture)
		{
			var item = value as CollectionModel;
			if (item == null)
				return new[] { new SolidColorBrush(Colors.Blue), new SolidColorBrush(Colors.Green) };
			
            Maximum = Math.Max(Maximum, item.Count);

			var percent = (item.Count == 0) ? 0 : (item.Count * 1.0 / Maximum * 1.0);
			percent = Math.Max(0, percent);
			percent = Math.Min(1, percent);

			var brush = TemplateColorProvider.Instance.ColorFrom(item.Name);

			var top = (int)(percent * MaximumNumberOfItems);
			top = Math.Min(top, item.Count);
			if (item.Count != 0) top = Math.Max(top, 1);

			return Enumerable.Range(1, top).Select(x => brush);
		}

		public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
		{
			throw new NotImplementedException();
		}
	}
}