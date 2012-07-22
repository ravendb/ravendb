using System;
using System.Collections;
using System.Linq;
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
        private static WeakEventListener<BindSelectedItemsBehavior, object, DesiredSelectionChangedEventArgs> desiredSelectionChangedEventListener;

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
            else if (AssociatedObject is ListBox)
            {
                return ((ListBox)AssociatedObject).SelectedItems;
            }
            else
            {
                return null;
            }
        }

        private Action<IEnumerable> GetSelectedItemsListUpdater()
        {
            if (AssociatedObject is DataGrid)
            {
                return GetListUpdater(((DataGrid) AssociatedObject).SelectedItems);
            }
            else if (AssociatedObject is ListBox && ((ListBox)AssociatedObject).SelectionMode != SelectionMode.Single)
            {
                return GetListUpdater(((ListBox)AssociatedObject).SelectedItems);
            }
            else if (AssociatedObject is ListBox)
            {
                return
                    (newSelection) =>
                    (AssociatedObject as ListBox).SelectedItem = newSelection.Cast<object>().FirstOrDefault();
            }
            else
            {
                return null;
            }
        }

        private Action<IEnumerable> GetListUpdater(IList selectedItems)
        {
            return newSelection =>
                       {
                           selectedItems.Clear();

                           foreach (var item in newSelection)
                           {
                               selectedItems.Add(item);
                           }
                       };
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

           if (desiredSelectionChangedEventListener != null)
           {
               desiredSelectionChangedEventListener.Detach();
           }

            if (e.NewValue != null)
            {
                var itemSelection = (e.NewValue as ItemSelection);

                desiredSelectionChangedEventListener = new WeakEventListener<BindSelectedItemsBehavior, object, DesiredSelectionChangedEventArgs>(
                    behavior)
                                                           {
                                                               OnDetachAction = (l => itemSelection.DesiredSelectionChanged -= l.OnEvent),
                                                               OnEventAction = (b, s, eventArgs) => b.HandleDesiredSelectionChanged(s,eventArgs)
                                                           };

                itemSelection.DesiredSelectionChanged += desiredSelectionChangedEventListener.OnEvent;
            }
        }


        private void HandleDesiredSelectionChanged(object sender, DesiredSelectionChangedEventArgs e)
        {
            var selectedItemsListUpdater = GetSelectedItemsListUpdater();

            if (selectedItemsListUpdater != null)
            {
                selectedItemsListUpdater(e.Items);
            }
        }
    }
}
