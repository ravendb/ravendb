using System.Linq;
using System.Windows.Controls;
using System.Windows.Input;
using System.Windows.Interactivity;
using System.Windows.Media;

namespace Raven.Studio.Behaviors
{
    public class SelectRowOnRightClickBehavior : Behavior<DataGrid>
    {
        protected override void OnAttached()
        {
            base.OnAttached();

            AssociatedObject.MouseRightButtonDown += HandleRightButtonClick;
        }

        protected override void OnDetaching()
        {
            base.OnDetaching();

            AssociatedObject.MouseRightButtonDown -= HandleRightButtonClick;
        }

        private void HandleRightButtonClick(object sender, MouseButtonEventArgs e)
        {
            var elementsUnderMouse = VisualTreeHelper.FindElementsInHostCoordinates(e.GetPosition(null), AssociatedObject);

            var row = elementsUnderMouse
                .OfType<DataGridRow>()
                .FirstOrDefault();

            if (row != null)
            {
                AssociatedObject.SelectedItem = row.DataContext;
            }
            else
            {
                AssociatedObject.SelectedItem = null;
            }
        }
    }
}
