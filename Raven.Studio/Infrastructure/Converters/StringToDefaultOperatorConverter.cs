using System;
using System.Globalization;
using System.Windows.Data;
using Raven.Abstractions.Data;

namespace Raven.Studio.Infrastructure.Converters
{
	public class StringToDefaultOperatorConverter : IValueConverter
	{
		public object Convert(object value, Type targetType, object parameter, CultureInfo culture)
		{
			if (value.ToString() == "And")
				return "AND";
			return "OR";
		}

		public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
		{
			if (value.ToString() == "AND")
				return QueryOperator.And;
			return QueryOperator.Or;
		}
	}
}