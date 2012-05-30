using System;
using System.Collections;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Controls.Primitives;
using System.Windows.Interactivity;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Behaviors
{
    public class BindSelectedItemsBehavior : Behavior<Control>
    {
        private Func<IEnumerable> selectedItemsProvider;

        public static readonly DependencyProperty TargetProperty =
            DependencyProperty.Register("Target", typeof(ItemSelection), typeof(BindSelectedItemsBehavior), new PropertyMetadata(default(ItemSelection), HandleItemSelectionChanged));

        public BindSelectedItemsBehavior()
        {
            selectedItemsProvider = GetSelectedItems;
        }

        private IEnumerable GetSelectedItems()
        {
            var selectedItemsList = GetSelectedItemsList();

            if (selectedItemsList != null)
            {
                return selectedItemsList;
            }
            else
            {
                return new object[0];
            }
        }

        private IList GetSelectedItemsList()
        {
            if (AssociatedObject is DataGrid)
            {
                return ((DataGrid)AssociatedObject).SelectedItems;
            }
            else if (AssociatedObject is Selector)
            {
                return ((ListBox)AssociatedObject).SelectedItems;
            }
            else
            {
                return null;
            }
        }

        protected override void OnAttached()
        {
            if (AssociatedObject is DataGrid)
            {
                ((DataGrid)AssociatedObject).SelectionChanged += HandleSelectionChanged;
            }
            else if (AssociatedObject is Selector)
            {
                ((ListBox)AssociatedObject).SelectionChanged += HandleSelectionChanged;
            }

            UpdateTarget();
        }

        protected override void OnDetaching()
        {
            if (AssociatedObject is DataGrid)
            {
                ((DataGrid)AssociatedObject).SelectionChanged -= HandleSelectionChanged;
            }
            else if (AssociatedObject is Selector)
            {
                ((ListBox)AssociatedObject).SelectionChanged -= HandleSelectionChanged;
            }
        }

        private void HandleSelectionChanged(object sender, SelectionChangedEventArgs e)
        {
            UpdateTarget();
        }

        private void UpdateTarget()
        {
            if (Target != null)
            {
                Target.NotifySelectionChanged(GetSelectedItemsCount(), selectedItemsProvider);
            }
        }

        private int GetSelectedItemsCount()
        {
            var selectedItemsList = GetSelectedItemsList();

            if (selectedItemsList != null)
            {
                return selectedItemsList.Count;
            }
            else
            {
                return 0;
            }
        }

        public ItemSelection Target
        {
            get { return (ItemSelection)GetValue(TargetProperty); }
            set { SetValue(TargetProperty, value); }
        }

        private static void HandleItemSelectionChanged(DependencyObject d, DependencyPropertyChangedEventArgs e)
        {
            var behavior = d as BindSelectedItemsBehavior;

            if (e.OldValue != null)
            {
                (e.OldValue as ItemSelection).DesiredSelectionChanged -= behavior.HandleDesiredSelectionChanged;
            }

            if (e.NewValue != null)
            {
                (e.NewValue as ItemSelection).DesiredSelectionChanged += behavior.HandleDesiredSelectionChanged;
            }
        }


        private void HandleDesiredSelectionChanged(object sender, DesiredSelectionChangedEventArgs e)
        {
            var selectedItemsList = GetSelectedItemsList();

            if (selectedItemsList != null)
            {
                selectedItemsList.Clear();

                foreach (var item in e.Items)
                {
                    selectedItemsList.Add(item);
                }
            }
        }
    }
}
