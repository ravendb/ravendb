namespace Raven.ManagementStudio.UI.Silverlight.Converters
{
    using System;
    using System.Globalization;
    using System.Windows.Data;
    using System.Windows;

    public class BooleanToVisibilityConverter : IValueConverter
    {
        public object Convert(object value, Type targetType, object parameter, CultureInfo culture)
        {
            if (value == null)
                return null;
            var booleanValue = (bool)value;
            return booleanValue ? Visibility.Visible : Visibility.Collapsed;
        }

        public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
        {
            throw new NotImplementedException();
        }
    }
}