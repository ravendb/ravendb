using System;
using System.Net;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using System.Windows.Documents;
using System.Windows.Ink;
using System.Windows.Input;
using System.Windows.Interactivity;
using System.Windows.Media;
using System.Windows.Media.Animation;
using System.Windows.Shapes;
using Raven.Studio.Controls;

namespace Raven.Studio.Infrastructure
{
    public class ProvideVisibleItemRangeFromItemsControlBehavior : Behavior<ItemsControl>
    {
        private static readonly DependencyProperty ItemsSourceProperty =
            DependencyProperty.Register("ItemsSource", typeof (object), typeof (ProvideVisibleItemRangeFromItemsControlBehavior), new PropertyMetadata(default(object), HandleItemsSourceChanged));

        private IEnquireAboutItemVisibility _cachedEnquirer;
        private bool _isLoaded;

        private static void HandleItemsSourceChanged(DependencyObject d, DependencyPropertyChangedEventArgs e)
        {
            var behavior = d as ProvideVisibleItemRangeFromItemsControlBehavior;

            behavior.HandleItemsSourceChanged();
        }

        private void HandleItemsSourceChanged()
        {
            if (_isLoaded)
            {
                DetachFromCachedEnquirer();
                AttachToEnquirer();
            }
        }

        private void HandleQueryItemVisibility(object sender, QueryItemVisibilityEventArgs e)
        {
            var wrapPanel = AssociatedObject.GetItemsHost() as VirtualizingWrapPanel;
            if (wrapPanel != null)
            {
                var range = wrapPanel.GetVisibleItemsRange();
                e.SetVisibleRange(range.FirstRealizedItemIndex, range.LastRealizedItemIndex);
            }
        }

        protected override void OnAttached()
        {
            base.OnAttached();

            BindingOperations.SetBinding(this, ItemsSourceProperty,
                                         new Binding("ItemsSource") {Source = AssociatedObject});

            AssociatedObject.Loaded += HandleLoaded;
            AssociatedObject.Unloaded += HandleUnloaded;
        }

        private void HandleLoaded(object sender, RoutedEventArgs e)
        {
            AttachToEnquirer();
            _isLoaded = true;
        }

        private void AttachToEnquirer()
        {
            _cachedEnquirer = GetValue(ItemsSourceProperty) as IEnquireAboutItemVisibility;
            if (_cachedEnquirer != null)
            {
                _cachedEnquirer.QueryItemVisibility += HandleQueryItemVisibility;
            }
        }

        private void HandleUnloaded(object sender, RoutedEventArgs e)
        {
            DetachFromCachedEnquirer();
            _isLoaded = false;
        }

        protected override void OnDetaching()
        {
            base.OnDetaching();

           ClearValue(ItemsSourceProperty);

           AssociatedObject.Loaded -= HandleLoaded;
           AssociatedObject.Unloaded -= HandleUnloaded;

           DetachFromCachedEnquirer();
        }

        private void DetachFromCachedEnquirer()
        {
            if (_cachedEnquirer != null)
            {
                _cachedEnquirer.QueryItemVisibility -= HandleQueryItemVisibility;
            }
        }
    }
}
