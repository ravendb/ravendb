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
    public class ProvideVisibleItemRangeBehavior : Behavior<ItemsControl>
    {
        private static readonly DependencyProperty ItemsSourceProperty =
            DependencyProperty.Register("ItemsSource", typeof (object), typeof (ProvideVisibleItemRangeBehavior), new PropertyMetadata(default(object), HandleItemsSourceChanged));

        private static void HandleItemsSourceChanged(DependencyObject d, DependencyPropertyChangedEventArgs e)
        {
            var behavior = d as ProvideVisibleItemRangeBehavior;

            var oldEnquirer = e.OldValue as IEnquireAboutItemVisibility;
            if (oldEnquirer != null)
            {
                oldEnquirer.QueryItemVisibility -= behavior.HandleQueryItemVisibility;
            }

            var newEnquirer = e.NewValue as IEnquireAboutItemVisibility;
            if (newEnquirer != null)
            {
                newEnquirer.QueryItemVisibility += behavior.HandleQueryItemVisibility;
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
        }

        protected override void OnDetaching()
        {
            base.OnDetaching();

           ClearValue(ItemsSourceProperty);
        }
    }
}
