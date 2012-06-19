using System;
using System.Net;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Controls.Primitives;
using System.Windows.Documents;
using System.Windows.Ink;
using System.Windows.Input;
using System.Windows.Interactivity;
using System.Windows.Media;
using System.Windows.Media.Animation;
using System.Windows.Shapes;

namespace Raven.Studio.Behaviors
{
    public class ShowPopupWhilstMouseOver : Behavior<FrameworkElement>
    {
        public static readonly DependencyProperty PopupProperty =
            DependencyProperty.Register("Popup", typeof (Popup), typeof (ShowPopupWhilstMouseOver), new PropertyMetadata(default(Popup)));

        public Popup Popup
        {
            get { return (Popup) GetValue(PopupProperty); }
            set { SetValue(PopupProperty, value); }
        }

        protected override void OnAttached()
        {
            AssociatedObject.MouseEnter += HandleMouseEnter;
            AssociatedObject.MouseLeave += HandleMouseLeave;
        }

        private void HandleMouseLeave(object sender, MouseEventArgs e)
        {
            if (Popup != null)
            {
                Popup.IsOpen = false;
            }
        }

        private void HandleMouseEnter(object sender, MouseEventArgs e)
        {
            if (Popup != null)
            {
                var parentBounds = AssociatedObject.GetBoundsRelativeTo(App.Current.RootVisual);

                Popup.VerticalOffset = parentBounds.Value.Bottom;
                Popup.HorizontalOffset = parentBounds.Value.Left;

                Popup.IsOpen = true;
            }
        }
    }
}
