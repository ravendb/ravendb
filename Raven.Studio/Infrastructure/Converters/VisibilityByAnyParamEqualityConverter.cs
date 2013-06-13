using System;
using System.Globalization;
using System.Linq;
using System.Windows;
using System.Windows.Data;

namespace Raven.Studio.Infrastructure.Converters
{
    public class VisibilityByAnyParamEqualityConverter : IValueConverter
    {
        public object Convert(object value, Type targetType, object parameter, CultureInfo culture)
        {
            int typedValue = System.Convert.ToInt32(value);
            int[] typedParameters = parameter.ToString().Split(',').Select(x => System.Convert.ToInt32((string)x)).ToArray();
            if (typedParameters.Any(typedValue.Equals))
            {
                return Visibility.Visible;
            }

            return Visibility.Collapsed;
        }

        public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
        {
            throw new NotSupportedException();
        }
    }
}