using System;
using System.Globalization;
using System.Windows.Data;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;

namespace Raven.Studio.Infrastructure.Converters
{
    public class DocumentPropertyToSingleLineStringConverter : IValueConverter
    {
        public static readonly DocumentPropertyToSingleLineStringConverter Default = new DocumentPropertyToSingleLineStringConverter();
        public static readonly DocumentPropertyToSingleLineStringConverter Trimmed = new DocumentPropertyToSingleLineStringConverter() { MaxLength = 200};

        public int MaxLength { get; set; }

        public object Convert(object value, Type targetType, object parameter, CultureInfo culture)
        {
            string stringValue;

            if (value is string)
            {
                stringValue = value as string;
                stringValue = stringValue.Replace(Environment.NewLine, " ").Trim('"');
            }
            else if (value is RavenJToken)
            {
                stringValue = (value as RavenJToken).ToString(Formatting.None).Trim('"');
            }
            else if (value != null)
            {
                stringValue = value.ToString();
            }
            else
            {
                stringValue = "";
            }

            if (MaxLength > 0 && stringValue.Length > MaxLength)
            {
                stringValue = stringValue.Substring(0,MaxLength) + "...";
            }

            return stringValue;
        }

        public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
        {
            throw new NotImplementedException();
        }
    }
}
