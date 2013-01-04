using System;
using System.Globalization;
using System.Windows;
using System.Windows.Data;
using System.Linq;

namespace Raven.Studio.Infrastructure.Converters
{
    public class DataTemplateSelectorConverter : IValueConverter
    {
        private TargetedDataTemplateCollection _targetedDataTemplates;

        public TargetedDataTemplateCollection DataTemplates
        {
            get { return _targetedDataTemplates ?? (_targetedDataTemplates = new TargetedDataTemplateCollection()); }
            set { _targetedDataTemplates = value; }
        }

        public object Convert(object value, Type targetType, object parameter, CultureInfo culture)
        {
            if (value == null || value == DependencyProperty.UnsetValue)
                return null;

            var type = value.GetType();
            var template = DataTemplates.Where(dt => dt.TargetType == type.FullName).FirstOrDefault();

            return template.Template;
        }

        public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
        {
            throw new NotImplementedException();
        }
    }
}