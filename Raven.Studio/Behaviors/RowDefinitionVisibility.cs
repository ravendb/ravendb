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
    public static class RowDefinitionVisibility
    {
        public static bool GetIsVisible(DependencyObject obj)
        {
            return (bool)obj.GetValue(IsVisibleProperty);
        }

        public static void SetIsVisible(DependencyObject obj, bool value)
        {
            obj.SetValue(IsVisibleProperty, value);
        }

        // Using a DependencyProperty as the backing store for IsVisible.  This enables animation, styling, binding, etc...
        public static readonly DependencyProperty IsVisibleProperty =
            DependencyProperty.RegisterAttached("IsVisible", typeof(bool), typeof(RowDefinitionVisibility), new PropertyMetadata(true, HandlePropertyChanged));

        private static GridLength GetCachedGridLengthProperty(DependencyObject obj)
        {
            return (GridLength)obj.GetValue(CachedGridLengthProperty);
        }

        private static void SetCachedGridLengthProperty(DependencyObject obj, GridLength value)
        {
            obj.SetValue(CachedGridLengthProperty, value);
        }

        private static readonly DependencyProperty CachedGridLengthProperty =
            DependencyProperty.RegisterAttached("CachedGridLengthProperty", typeof(GridLength), typeof(RowDefinitionVisibility), new PropertyMetadata(new GridLength(0)));

        
        private static void HandlePropertyChanged(DependencyObject d, DependencyPropertyChangedEventArgs e)
        {
            var rowDefinition = d as RowDefinition;
            if (rowDefinition == null)
            {
                return;
            }

            var isVisible = (bool)e.NewValue;

            if (isVisible)
            {
                rowDefinition.Height = GetCachedGridLengthProperty(rowDefinition);
            }
            else
            {
                SetCachedGridLengthProperty(rowDefinition, rowDefinition.Height);
                rowDefinition.Height = new GridLength(0);
            }
        }
    }
}
