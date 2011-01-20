using System.Windows.Data;
using Raven.ManagementStudio.Plugin;

namespace Raven.ManagementStudio.UI.Silverlight.Converters
{
    public class CategoryIsActiveConverter : IValueConverter
    {
        public object Convert(object value, System.Type targetType, object parameter, System.Globalization.CultureInfo culture)
        {
            var screen = value as IRavenScreen;
            if (screen != null)
            {
                return screen.Section.ToString() == parameter as string;
            }
            return false;
        }

        public object ConvertBack(object value, System.Type targetType, object parameter, System.Globalization.CultureInfo culture)
        {
            throw new System.NotImplementedException();
        }
    }
}