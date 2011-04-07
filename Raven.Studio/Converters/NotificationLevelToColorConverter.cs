namespace Raven.Studio.Converters
{
	using System;
	using System.Globalization;
	using System.Linq;
	using System.Windows;
	using System.Windows.Data;
	using System.Windows.Media;
	using Messages;

	public class NotificationLevelToColorConverter : IValueConverter
	{
		public object Convert(object value, Type targetType, object parameter, CultureInfo culture)
		{
			NotificationLevel level;
			if (!Enum.TryParse(value.ToString(), out level)) return null;

			var styleName = value + "Color";

			var color = FindResource(styleName);

			return color ?? Colors.Magenta;
		}

		public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture) { throw new NotImplementedException(); }

		static object FindResource(string name)
		{
			return RecurseDictionaries(Application.Current.Resources, name);
		}

		static object RecurseDictionaries(ResourceDictionary dictionary, string name)
		{
			if (dictionary.Contains(name)) return dictionary[name];

			return dictionary.MergedDictionaries
				.Select(child => RecurseDictionaries(child, name))
				.FirstOrDefault(candidate => candidate != null);
		}
	}
}