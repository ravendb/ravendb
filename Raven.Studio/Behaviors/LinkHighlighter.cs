using System.Windows;

namespace Raven.Studio.Behaviors
{
    public static class LinkHighlighter
    {
        public static readonly DependencyProperty AlternativeUrisProperty =
            DependencyProperty.RegisterAttached("AlternativeUris", typeof(StringCollection), typeof(LinkHighlighter), new PropertyMetadata(null));
    
        public static StringCollection GetAlternativeUris(DependencyObject dependencyObject)
        {
            return (StringCollection) dependencyObject.GetValue(AlternativeUrisProperty);
        }

        public static void SetAlternativeUris(DependencyObject dependencyObject, StringCollection value)
        {
            dependencyObject.SetValue(AlternativeUrisProperty, value);
        }
    }
}