using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Globalization;
using System.Linq;
using System.Windows.Data;

namespace Raven.Studio.Infrastructure.Converters
{
	public class FromEnumWithDescription : IValueConverter
	{
		public object Convert(object value, Type targetType, object parameter, CultureInfo culture)
		{
			var type = value.GetType();

			var fields = type.GetFields().Where(field => field.IsLiteral);

			foreach (var field in fields)
			{
				if (field.Name == value.ToString())
				{
					var a = (DescriptionAttribute[]) field.GetCustomAttributes(typeof (DescriptionAttribute), false);
					if (a != null && a.Length > 0)
						return a[0].Description;
					return value.ToString();
				}
			}

			return null;
		}

		readonly Dictionary<Type, Dictionary<string, string>> cache = new Dictionary<Type, Dictionary<string, string>>();

		public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
		{
			var description = value as string;
			if (description == null)
				return null;

			var fields = targetType.GetFields().Where(field => field.IsLiteral);
			if (!cache.ContainsKey(targetType))
			{
				cache[targetType] = new Dictionary<string, string>();
				foreach (var field in fields)
				{
					var a = (DescriptionAttribute[]) field.GetCustomAttributes(typeof (DescriptionAttribute), false);
					if (a != null && a.Length > 0)
						cache[targetType].Add(a[0].Description, field.Name);
					else
						cache[targetType].Add(field.Name, field.Name);
				}
			}

			return cache[targetType][description];
		}
	}
}
