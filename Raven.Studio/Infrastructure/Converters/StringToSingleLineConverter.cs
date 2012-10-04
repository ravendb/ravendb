using System;
using System.Globalization;
using System.Windows.Data;

namespace Raven.Studio.Infrastructure.Converters
{
    public class StringToSingleLineConverter : IValueConverter
    {
        public object Convert(object value, Type targetType, object parameter, CultureInfo culture)
        {
            var stringValue = value as string;
            if (stringValue == null)
            {
                return string.Empty;
            }

            return stringValue
                .Replace(" " + Environment.NewLine, " ")
                .Replace(Environment.NewLine, " ");
        }

        public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
        {
            throw new NotImplementedException();
        }
    }
}