namespace Raven.Studio.Converters
{
	using System;
	using System.Collections.Generic;
	using System.ComponentModel;
	using System.Globalization;
	using System.Linq;
	using System.Reflection;
	using System.Windows.Data;

	public class EnumToEnumerableConverter : IValueConverter
	{
		readonly Dictionary<Type, List<object>> cache = new Dictionary<Type, List<object>>();

		public object Convert(object value, Type targetType, object parameter, CultureInfo culture)
		{
			Type type = value.GetType();
			if (!cache.ContainsKey(type))
			{
				IEnumerable<FieldInfo> fields = type.GetFields().Where(field => field.IsLiteral);
				var values = new List<object>();
				foreach (FieldInfo field in fields)
				{
					var a = (DescriptionAttribute[]) field.GetCustomAttributes(typeof (DescriptionAttribute), false);
					if (a != null && a.Length > 0)
					{
						values.Add(a[0].Description);
					}
					else
					{
						values.Add(field.GetValue(value));
					}
				}
				cache[type] = values;
			}

			return cache[type];
		}

		public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
		{
			throw new NotImplementedException();
		}
	}
}