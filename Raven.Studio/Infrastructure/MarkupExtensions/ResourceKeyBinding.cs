using System;
using System.Globalization;
using System.Windows;
using System.Windows.Data;
using System.Windows.Markup;

namespace Raven.Studio.Infrastructure.MarkupExtensions
{
    public class ResourceKeyBindingExtension : MarkupExtension
    {
        public string Path { get; set; }

        public override object ProvideValue(IServiceProvider serviceProvider)
        {
            var provideTarget = serviceProvider.GetService(typeof (IProvideValueTarget)) as IProvideValueTarget;

            var target = provideTarget.TargetObject as FrameworkElement;
            if (target == null)
                return null;

            var binding = new Binding(Path) {Converter = new KeyToResourceConverter(target)};

            return binding;
        }

        private class KeyToResourceConverter : IValueConverter
        {
            private readonly FrameworkElement targetObject;

            public KeyToResourceConverter(FrameworkElement targetObject)
            {
                this.targetObject = targetObject;
            }

            public object Convert(object value, Type targetType, object parameter, CultureInfo culture)
            {
                return value == null ? null : targetObject.TryFindResource(value);
            }

            public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
            {
                throw new NotImplementedException();
            }
        }
    }
}