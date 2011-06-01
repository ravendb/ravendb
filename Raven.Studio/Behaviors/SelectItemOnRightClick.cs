namespace Raven.Studio.Behaviors {
    using System.Linq;
    using System.Windows.Controls;
    using System.Windows.Input;
    using System.Windows.Interactivity;
    using System.Windows.Media;

    public class SelectItemOnRightClick : Behavior<ListBox> {
        protected override void OnAttached() {
            AssociatedObject.MouseRightButtonDown += AssociatedObject_MouseRightButtonDown;
            base.OnAttached();
        }

        protected override void OnDetaching() {
            AssociatedObject.MouseRightButtonDown -= AssociatedObject_MouseRightButtonDown;
            base.OnDetaching();
        }

        void AssociatedObject_MouseRightButtonDown(object sender, MouseButtonEventArgs e) {
            var item = VisualTreeHelper.FindElementsInHostCoordinates(
                e.GetPosition(null),
                AssociatedObject
                ).OfType<ListBoxItem>().FirstOrDefault();

            if(item != null) {
                if (AssociatedObject.SelectedItems.Contains(item.DataContext))
                    return;

                AssociatedObject.SelectedItems.Clear();
                item.IsSelected = true;
            }
        }
    }
}