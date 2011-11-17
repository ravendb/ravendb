using System;
using System.Globalization;
using System.Windows;
using System.Windows.Data;

namespace Raven.Studio.Infrastructure.Converters
{
    public class StringEmptyToGridLengthConverter : IValueConverter
    {
        public GridLength ValueWhenEmpty { get; set; }

        public GridLength ValueWhenNonEmpty { get; set; }

        public object Convert(object value, Type targetType, object parameter, CultureInfo culture)
        {
            var stringValue = value as string;
            return string.IsNullOrEmpty(stringValue) ? ValueWhenEmpty : ValueWhenNonEmpty;
        }

        public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
        {
            throw new NotImplementedException();
        }
    }
}
