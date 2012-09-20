using System;
using System.Net;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Documents;
using System.Windows.Ink;
using System.Windows.Input;
using System.Windows.Interactivity;
using System.Windows.Media;
using System.Windows.Media.Animation;
using System.Windows.Shapes;
using System.Windows.Controls.Primitives;
using System.Linq;
using ContextMenu = Raven.Studio.Infrastructure.ContextMenu.ContextMenu;

namespace Raven.Studio.Behaviors
{
    public class ColumnHeaderContextMenu : Behavior<DataGrid>
    {
        public static readonly DependencyProperty ContextMenuProperty =
            DependencyProperty.Register("ContextMenu", typeof (ContextMenu), typeof (ColumnHeaderContextMenu), new PropertyMetadata(default(ContextMenu)));

        public ContextMenu ContextMenu
        {
            get { return (ContextMenu) GetValue(ContextMenuProperty); }
            set { SetValue(ContextMenuProperty, value); }
        }

        protected override void OnAttached()
        {
            base.OnAttached();

            AssociatedObject.Loaded += HandleLoaded;
            AssociatedObject.Unloaded += HandleUnloaded;
        }

        private void HandleUnloaded(object sender, RoutedEventArgs e)
        {
            if (ContextMenu != null)
            {
                ContextMenu.Owner = null;
            }
        }

        private void HandleLoaded(object sender, RoutedEventArgs e)
        {
            var columnHeadersPresenter = AssociatedObject.GetVisualDescendants().OfType<DataGridColumnHeadersPresenter>().FirstOrDefault();

            if (columnHeadersPresenter != null && columnHeadersPresenter.Background == null)
            {
                columnHeadersPresenter.Background = new SolidColorBrush(Colors.Transparent);
            }

            if (ContextMenu != null)
            {
                ContextMenu.Owner = columnHeadersPresenter;
            }
        }

        protected override void OnDetaching()
        {
            base.OnDetaching();

            AssociatedObject.Loaded -= HandleLoaded;
            AssociatedObject.Unloaded -= HandleUnloaded;
        }
    }
}
