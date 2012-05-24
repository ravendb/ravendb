using System;
using System.Globalization;
using System.Net;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using System.Windows.Documents;
using System.Windows.Ink;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Animation;
using System.Windows.Shapes;
using System.Linq;

namespace Raven.Studio.Infrastructure.Converters
{
    public class StringValueToObjectConverter : IValueConverter
    {
        private StringValueConversionCollection conversions;

        public StringValueConversionCollection Conversions
        {
            get { return conversions ?? (conversions = new StringValueConversionCollection()); }
            set { conversions = value; }
        }

        public object Convert(object value, Type targetType, object parameter, CultureInfo culture)
        {
            if (value == null)
            {
                return null;
            }

            var selector = value.ToString();

            var conversion = Conversions.FirstOrDefault(c => c.When == selector);

            return conversion != null ? conversion.Then : null;
        }

        public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
        {
            throw new NotImplementedException();
        }
    }
}
