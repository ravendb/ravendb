using System;
using System.Globalization;
using System.Windows;
using System.Windows.Data;

namespace Raven.Studio.Infrastructure.Converters
{
    public class CollectionCountToGridLengthConverter : IValueConverter
    {
        public object Convert(object value, Type targetType, object parameter, CultureInfo culture)
        {
            if (!(value is int))
                return GridLength.Auto;
            
            var count = (int) value;
            return count == 0 ? new GridLength(0) : new GridLength(1, GridUnitType.Star);
        }

        public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
        {
            throw new NotImplementedException();
        }
    }
}