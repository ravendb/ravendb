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
using Newtonsoft.Json;
using Raven.Json.Linq;

namespace Raven.Studio.Infrastructure.Converters
{
    public class DocumentPropertyToSingleLineStringConverter : IValueConverter
    {
        public static readonly DocumentPropertyToSingleLineStringConverter Default = new DocumentPropertyToSingleLineStringConverter();

        public object Convert(object value, Type targetType, object parameter, CultureInfo culture)
        {
            if (value is string)
            {
                var stringValue = value as string;
                return stringValue.Replace(Environment.NewLine, " ").Trim('"');
            }
            else if (value is RavenJToken)
            {
                return (value as RavenJToken).ToString(Formatting.None).Trim('"');
            }
            else
            {
                return value;
            }
        }

        public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
        {
            throw new NotImplementedException();
        }
    }
}
