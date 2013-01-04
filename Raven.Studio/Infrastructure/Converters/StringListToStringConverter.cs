using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Windows.Data;

namespace Raven.Studio.Infrastructure.Converters
{
    public class StringListToStringConverter : IValueConverter
    {
        public object Convert(object value, Type targetType, object parameter, CultureInfo culture)
        {
            var list = value as IEnumerable<string>;
            if (list == null)
                return string.Empty;
            return string.Join(",", list);
        }

        public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
        {
            var str = value as string;
            if (str == null)
                return null;
            return str.Split(',').ToList();
        }
    }
}