using System;
using System.Globalization;
using System.Windows.Data;
using Raven.Studio.Extensions;

namespace Raven.Studio.Infrastructure.Converters
{
    public class TrimmedStringConverter : IValueConverter
    {
        public object Convert(object value, Type targetType, object parameter, CultureInfo culture)
        {
            if (!(value is string))
                return value;

            return ((string) value).TrimmedViewOfString(System.Convert.ToInt32(parameter));
        }

        public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
        {
            throw new NotImplementedException();
        }
    }
}