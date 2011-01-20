using System.Windows;
using System.Windows.Controls;

namespace Raven.ManagementStudio.UI.Silverlight.Controls
{
    public class HyperlinkHelper
    {
        public static bool? GetIsActive(DependencyObject obj)
        {
            return (bool?)obj.GetValue(IsActiveProperty);
        }

        public static void SetIsActive(DependencyObject obj, bool? value)
        {
            obj.SetValue(IsActiveProperty, value);
        }

        public static readonly DependencyProperty IsActiveProperty =
            DependencyProperty.RegisterAttached("IsActive", typeof(bool?), typeof(HyperlinkHelper), new PropertyMetadata(null, OnIsActiveChanged));

        private static void OnIsActiveChanged(DependencyObject d, DependencyPropertyChangedEventArgs e)
        {
            var control = (Control)d;
            if (!SetState(control, (bool?)e.NewValue))
            {
                control.Loaded += OnControlLoaded;
            }
        }

        private static void OnControlLoaded(object sender, RoutedEventArgs e)
        {
            var control = (Control)sender;
            control.Loaded -= OnControlLoaded;
            SetState(control, GetIsActive(control));
        }

        private static bool SetState(Control control, bool? value)
        {
            return VisualStateManager.GoToState(control, value == true ? "ActiveLink" : "InactiveLink", true);
        }
    }
}
