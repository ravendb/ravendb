namespace Raven.ManagementStudio.UI.Silverlight.Converters
{
    using System;
    using System.Globalization;
    using System.Windows.Data;

    public class EnumToIntConverter : IValueConverter
    {
        #region IValueConverter Members

        public object Convert(object value, Type targetType, object parameter, CultureInfo culture)
        {
            return (int) value;
        }

        public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
        {
            return Enum.Parse(targetType, value.ToString(), true);
        }

        #endregion
    }
}