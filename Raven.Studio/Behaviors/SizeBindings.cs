using System;
using System.Net;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Documents;
using System.Windows.Ink;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Animation;
using System.Windows.Shapes;

namespace Raven.Studio.Behaviors
{
    public static class SizeBindings
    {
        public static readonly DependencyProperty ActualHeightProperty =
            DependencyProperty.RegisterAttached("ActualHeight", typeof (double), typeof (SizeBindings),
                                                new PropertyMetadata(0.0));

        public static readonly DependencyProperty ActualWidthProperty =
            DependencyProperty.RegisterAttached("ActualWidth", typeof (Double), typeof (SizeBindings),
                                                new PropertyMetadata(0.0));

        public static readonly DependencyProperty IsEnabledProperty =
            DependencyProperty.RegisterAttached("IsEnabled", typeof (bool), typeof (SizeBindings),
                                                new PropertyMetadata(false, HandlePropertyChanged));

        private static void HandlePropertyChanged(DependencyObject d, DependencyPropertyChangedEventArgs e)
        {
            var element = d as FrameworkElement;
            if (element == null)
            {
                return;
            }

            if ((bool) e.NewValue == false)
            {
                element.SizeChanged -= HandleSizeChanged;
            }
            else
            {
                element.SizeChanged += HandleSizeChanged;
            }
        }

        private static void HandleSizeChanged(object sender, SizeChangedEventArgs e)
        {
            var element = sender as FrameworkElement;

            SetActualHeight(element, e.NewSize.Height);
            SetActualWidth(element, e.NewSize.Width);
        }

        public static bool GetIsEnabled(DependencyObject obj)
        {
            return (bool)obj.GetValue(IsEnabledProperty);
        }

        public static void SetIsEnabled(DependencyObject obj, bool value)
        {
            obj.SetValue(IsEnabledProperty, value);
        }

        public static Double GetActualWidth(DependencyObject obj)
        {
            return (Double) obj.GetValue(ActualWidthProperty);
        }

        public static void SetActualWidth(DependencyObject obj, Double value)
        {
            obj.SetValue(ActualWidthProperty, value);
        }

        public static double GetActualHeight(DependencyObject obj)
        {
            return (double)obj.GetValue(ActualHeightProperty);
        }

        public static void SetActualHeight(DependencyObject obj, double value)
        {
            obj.SetValue(ActualHeightProperty, value);
        }
    }
}
