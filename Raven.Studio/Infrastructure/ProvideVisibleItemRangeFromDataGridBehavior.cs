using System;
using System.Collections.Generic;
using System.Linq;
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
    public class ProvideVisibleItemRangeFromDataGridBehavior : Behavior<DataGrid>
    {
        private static readonly DependencyProperty ItemsSourceProperty =
            DependencyProperty.Register("ItemsSource", typeof(object), typeof(ProvideVisibleItemRangeFromDataGridBehavior), new PropertyMetadata(default(object), HandleItemsSourceChanged));

        private IEnquireAboutItemVisibility _cachedEnquirer;
        private bool _isLoaded;
        private HashSet<int> _loadedRows = new HashSet<int>();

        private static void HandleItemsSourceChanged(DependencyObject d, DependencyPropertyChangedEventArgs e)
        {
            var behavior = d as ProvideVisibleItemRangeFromDataGridBehavior;

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
            if (_loadedRows.Count > 0)
            {
                e.SetVisibleRange(_loadedRows.Min(), _loadedRows.Max());
            }
        }

        protected override void OnAttached()
        {
            base.OnAttached();

            BindingOperations.SetBinding(this, ItemsSourceProperty,
                                         new Binding("ItemsSource") {Source = AssociatedObject});

            AssociatedObject.Loaded += HandleLoaded;
            AssociatedObject.Unloaded += HandleUnloaded;
            AssociatedObject.LoadingRow += HandleLoadingRow;
            AssociatedObject.UnloadingRow += HandleUnloadingRow;
        }

        private void HandleUnloadingRow(object sender, DataGridRowEventArgs e)
        {
            _loadedRows.Remove(e.Row.GetIndex());
        }

        private void HandleLoadingRow(object sender, DataGridRowEventArgs e)
        {
            _loadedRows.Add(e.Row.GetIndex());
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
           AssociatedObject.LoadingRow -= HandleLoadingRow;
           AssociatedObject.UnloadingRow -= HandleUnloadingRow;

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
