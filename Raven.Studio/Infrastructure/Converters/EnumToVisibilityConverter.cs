using System;
using System.Globalization;
using System.Linq;
using System.Windows;
using System.Windows.Data;

namespace Raven.Studio.Infrastructure.Converters
{
    public class EnumToVisibilityConverter : IValueConverter
    {
        private string valuesWhenVisibile;
        private string valuesWhenCollapsed;

        public Type Type { get; set; }
        private object[] parsedVisibleValues;
        private object[] parsedCollapsedValues;
        
        public string ValuesWhenVisibile
        {
            get { return valuesWhenVisibile ?? string.Empty; }
            set
            {
                valuesWhenVisibile = value;
                parsedVisibleValues = null;
            }
        }


        public string ValuesWhenCollapsed
        {
            get { return valuesWhenCollapsed ?? string.Empty; ; }
            set
            {
                valuesWhenCollapsed = value;
                parsedCollapsedValues = null;
            }
        }

        public object Convert(object value, Type targetType, object parameter, CultureInfo culture)
        {
            var enumValuesWhenVisible = GetValuesWhenVisible();
            if (enumValuesWhenVisible.Any())
            {
                return enumValuesWhenVisible.Contains(value) ? Visibility.Visible : Visibility.Collapsed;
            }
            else
            {
                return GetValuesWhenCollapsed().Contains(value) ? Visibility.Collapsed : Visibility.Visible;
            }
        }

        private object[] GetValuesWhenVisible()
        {
            if (parsedVisibleValues == null)
            {
                var enumValues = ParseEnumValues(ValuesWhenVisibile);

                parsedVisibleValues = enumValues;
            }

            return parsedVisibleValues;
        }

        private object[] GetValuesWhenCollapsed()
        {
            if (parsedCollapsedValues == null)
            {
                var enumValues = ParseEnumValues(ValuesWhenCollapsed);

                parsedCollapsedValues = enumValues;
            }

            return parsedCollapsedValues;
        }

        private object[] ParseEnumValues(string memberList)
        {
            var enumValues = memberList.Split(',')
                .Select(s => s.Trim())
                .Where(s => !string.IsNullOrEmpty(s))
                .Select(s => Enum.Parse(Type, s, ignoreCase: false))
                .ToArray();
            return enumValues;
        }

        public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
        {
            throw new NotImplementedException();
        }


    }
}
