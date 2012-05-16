using System;
using System.Linq;
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

namespace Raven.Studio.Behaviors
{
    public class BindColumnVisibility : Behavior<DataGrid>
    {
        public static readonly DependencyProperty ColumnHeaderProperty =
            DependencyProperty.Register("ColumnHeader", typeof (string), typeof (BindColumnVisibility), new PropertyMetadata(default(string), HandlePropertyChanged));

        public string ColumnHeader
        {
            get { return (string) GetValue(ColumnHeaderProperty); }
            set { SetValue(ColumnHeaderProperty, value); }
        }

        public static readonly DependencyProperty VisibilityProperty =
            DependencyProperty.Register("Visibility", typeof(Visibility), typeof(BindColumnVisibility), new PropertyMetadata(Visibility.Visible, HandlePropertyChanged));

        public Visibility Visibility
        {
            get { return (Visibility) GetValue(VisibilityProperty); }
            set { SetValue(VisibilityProperty, value); }
        }


        private static void HandlePropertyChanged(DependencyObject d, DependencyPropertyChangedEventArgs e)
        {
            (d as BindColumnVisibility).HandlePropertyChanged();
        }

        protected override void OnAttached()
        {
            base.OnAttached();

            HandlePropertyChanged();
        }

        private void HandlePropertyChanged()
        {
            if (AssociatedObject == null || ColumnHeader == null)
            {
                return;
            }

            var column = AssociatedObject.Columns.FirstOrDefault(c => (c.Header as string) == ColumnHeader);

            if (column != null)
            {
                column.Visibility = Visibility;
            }
        }
    }
}
