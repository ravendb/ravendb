using System.Windows;
using System.Windows.Media;

namespace Raven.Studio.Extensions
{
    public static class SilverlightExtensions
    {
        public static DependencyObject GetRoot(this DependencyObject child)
        {
            var parent = VisualTreeHelper.GetParent(child);
            if (parent != null)
                GetRoot(parent);
            return child;
        }

        public static object TryFindResource(this FrameworkElement element, object resourceKey)
        {
            var currentElement = element;

            while (currentElement != null)
            {
                var resource = currentElement.Resources[resourceKey];
                if (resource != null)
                {
                    return resource;
                }

                currentElement = currentElement.Parent as FrameworkElement;
            }

            return Application.Current.Resources[resourceKey];
        }
    }
}