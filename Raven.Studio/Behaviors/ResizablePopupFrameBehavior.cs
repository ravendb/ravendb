using System;
using System.ComponentModel;
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
    /// <summary>
    /// The available directions where the element can resize to.
    /// </summary>
    public enum FrameComponent
    {
        None,
        TitleBar,
        LeftEdge,
        RightEdge,
        TopEdge,
        BottomEdge,
        TopRightCorner,
        BottomRightCorner,
        TopLeftCorner,
        BottomLeftCorner,
    }

    public class ResizablePopupFrameBehavior : Behavior<FrameworkElement>
    {
        public Popup  Popup
        {
            get { return (Popup)GetValue(PopupProperty); }
            set { SetValue(PopupProperty, value); }
        }

        public static readonly DependencyProperty PopupProperty =
            DependencyProperty.Register("MyProperty", typeof(Popup), typeof(ResizablePopupFrameBehavior), new PropertyMetadata(null));


        public static Rect GetResizeBounds(DependencyObject obj)
        {
            return (Rect)obj.GetValue(ResizeBoundsProperty);
        }

        public static void SetResizeBounds(DependencyObject obj, Rect value)
        {
            obj.SetValue(ResizeBoundsProperty, value);
        }
        
        public static Popup GetParentPopup(DependencyObject obj)
        {
            return (Popup)obj.GetValue(ParentPopupProperty);
        }

        public static void SetParentPopup(DependencyObject obj, Popup value)
        {
            obj.SetValue(ParentPopupProperty, value);
        }

        public static readonly DependencyProperty ParentPopupProperty =
            DependencyProperty.RegisterAttached("ParentPopup", typeof(Popup), typeof(ResizablePopupFrameBehavior), new PropertyMetadata(null));

        
        public static readonly DependencyProperty ResizeBoundsProperty =
            DependencyProperty.RegisterAttached("ResizeBounds", typeof(Rect), typeof(ResizablePopupFrameBehavior), new PropertyMetadata(new Rect()));



        /// <summary>
        /// Flag to see if resizing is in progress.
        /// </summary>
        private bool _IsResizing = false;

        /// <summary>
        /// Point to remember the mouse movement change.
        /// </summary>
        private Point dragStart;

        /// <summary>
        /// Point for storing the size of the reziseable element.
        /// </summary>
        private Size _ResizeElementWindowSize = new Size();

        /// <summary>
        /// The AssociatedObject wich will be dragged for the resizing.
        /// Reference wil be kept in this property so events can be descriped on the detach.
        /// </summary>
        private FrameworkElement DragElement;

        /// <summary>
        /// Gets or Sets the value whether this behavior is active.
        /// </summary>
        [Category("Common Properties"), Description("The targeted element that must be resized."),
         EditorBrowsable(EditorBrowsableState.Advanced), DefaultValue(true)]
        public bool IsEnabled
        {
            get { return _IsEnabled; }
            set { _IsEnabled = value; }
        }

        private bool _IsEnabled = true;

        /// <summary>
        /// The direction the resizing must take place.
        /// </summary>
        [Category("Common Properties"), Description("The direction the resizing must take place.")]
        public FrameComponent FrameComponent
        {
            get { return (FrameComponent)GetValue(FrameComponentProperty); }
            set { SetValue(FrameComponentProperty, value); }
        }

        public static readonly DependencyProperty FrameComponentProperty =
            DependencyProperty.Register("FrameComponent", typeof(FrameComponent), typeof(ResizablePopupFrameBehavior),
                                        new PropertyMetadata(FrameComponent.None));
        private Point popupInitialPosition;
        private Size popupInitialSize;

        /// <summary>
        /// Sets the mouse cursor.
        /// </summary>
        private void SetResizeDirections()
        {
            if (DragElement == null) return;

            switch (FrameComponent)
            {
                case FrameComponent.LeftEdge:
                case FrameComponent.RightEdge:
                    DragElement.Cursor = Cursors.SizeWE;
                    break;
                case FrameComponent.TopEdge:
                case FrameComponent.BottomEdge:
                    DragElement.Cursor = Cursors.SizeNS;
                    break;
                case FrameComponent.TopLeftCorner:
                case FrameComponent.BottomRightCorner:
                    DragElement.Cursor = Cursors.SizeNWSE;
                    break;
                case FrameComponent.TopRightCorner:
                case FrameComponent.BottomLeftCorner:
                    DragElement.Cursor = Cursors.SizeNESW;
                    break;
                case FrameComponent.TitleBar:
                    DragElement.Cursor = Cursors.Hand;
                    break;
                default:
                    DragElement.Cursor = Cursors.Arrow;
                    break;
            }
        }

        /// <summary>
        /// Event when the behavior is attached to a element.
        /// </summary>
        protected override void OnAttached()
        {
            base.OnAttached();

            if (!IsEnabled) return;

            DragElement = AssociatedObject;

            DragElement.MouseLeftButtonDown += DragElementMouseLeftButtonDown;
            DragElement.MouseLeftButtonUp += DragElementMouseLeftButtonUp;
            DragElement.MouseMove += DragElementMouseMove;
            DragElement.MouseLeave += DragElementMouseLeave;

            SetResizeDirections();
        }

        /// <summary>
        /// Event when the behavior is detached from the element.
        /// </summary>
        protected override void OnDetaching()
        {
            base.OnDetaching();

            DragElement.MouseLeftButtonDown -= DragElementMouseLeftButtonDown;
            DragElement.MouseLeftButtonUp -= DragElementMouseLeftButtonUp;
            DragElement.MouseMove -= DragElementMouseMove;
            DragElement.MouseLeave -= DragElementMouseLeave;
        }

        /// <summary>
        /// Event when the left mouse button is down on the dragging element.
        /// Starts resizing.
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void DragElementMouseLeftButtonDown(object sender, MouseButtonEventArgs e)
        {
            if (CanResize)
            {
                // Capture the mouse
                ((FrameworkElement)sender).CaptureMouse();

                // Store the start position
                dragStart = e.GetPosition(Popup.Parent as UIElement);
                popupInitialPosition = new Point(Popup.HorizontalOffset, Popup.VerticalOffset);
                popupInitialSize = new Size(ResizedElement.Width, ResizedElement.Height);

                // Set resizing to true
                _IsResizing = true;
            }
        }

        private FrameworkElement ResizedElement
        {
            get { return Popup.Child as FrameworkElement; }
        }

        /// <summary>
        /// Event when the left mouse button is up on the dragging element.
        /// Stops resizing.
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void DragElementMouseLeftButtonUp(object sender, MouseButtonEventArgs e)
        {
            if (_IsResizing)
            {
                // Release the mouse
                ((FrameworkElement)sender).ReleaseMouseCapture();

                // Set resizing to false
                _IsResizing = false;
            }
        }

        /// <summary>
        /// Event when the mouse moves on the dragging element.
        /// Calculates the resizing when in dragging mode.
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void DragElementMouseMove(object sender, MouseEventArgs e)
        {
            if (!CanResize || !_IsResizing) return;

            var position = e.GetPosition(Popup.Parent as UIElement);
            Resize(position.X - dragStart.X, position.Y - dragStart.Y);

        }

        /// <summary>
        /// Event when the mouse leaves the draggin element.
        /// Stops dragging.
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void DragElementMouseLeave(object sender, MouseEventArgs e)
        {
            //_IsResizing = false;
        }

        /// <summary>
        /// Read-Only. Checks if all required properties are available for the resize ability.
        /// </summary>
        private bool CanResize
        {
            get
            {
                if (FrameComponent == FrameComponent.None)
                    return false;
                if (Popup == null)
                    return false;

                return true;
            }
        }

        /// <summary>
        /// Calculates the distance between the two points and resizes the resizeelement.
        /// </summary>
        /// <param name="mousePosition">Point referenced to element.</param>
        private void Resize(double deltaX, double deltaY)
        {
            if (Popup == null)
                return;

            if (FrameComponent == FrameComponent.TitleBar)
            {
                Popup.HorizontalOffset = popupInitialPosition.X + deltaX;
                Popup.VerticalOffset = popupInitialPosition.Y + deltaY;
            }
            if (FrameComponent == FrameComponent.RightEdge || FrameComponent == FrameComponent.TopRightCorner || FrameComponent == FrameComponent.BottomRightCorner)
            {
                if (popupInitialSize.Width + deltaX > 0)
                    ResizedElement.Width = popupInitialSize.Width + deltaX;
            }
            if (FrameComponent == FrameComponent.LeftEdge || FrameComponent == FrameComponent.TopLeftCorner || FrameComponent == FrameComponent.BottomLeftCorner)
            {
                if (popupInitialSize.Width + deltaX > 0)
                {
                    ResizedElement.Width = popupInitialSize.Width - deltaX;
                    Popup.HorizontalOffset =  popupInitialPosition.X + deltaX;
                }
            }
            if (FrameComponent == FrameComponent.TopEdge || FrameComponent == FrameComponent.TopLeftCorner || FrameComponent == FrameComponent.TopRightCorner)
            {
                if (popupInitialSize.Height + deltaY > 0)
                {
                    ResizedElement.Height = popupInitialSize.Height - deltaY;
                    Popup.VerticalOffset = popupInitialPosition.Y + deltaY;
                }
            }
            if (FrameComponent == FrameComponent.BottomEdge || FrameComponent == FrameComponent.BottomLeftCorner || FrameComponent == FrameComponent.BottomRightCorner)
            {
                if (popupInitialSize.Height + deltaY > 0)
                    ResizedElement.Height = popupInitialSize.Height + deltaY;
            }

            var bounds = GetResizeBounds(this);

            ResizedElement.Width = Math.Min(ResizedElement.Width, bounds.Width);
            ResizedElement.Height = Math.Min(ResizedElement.Height, bounds.Height);

            Popup.HorizontalOffset = Math.Max(bounds.Left,
                                              Math.Min(bounds.Right - ResizedElement.Width, Popup.HorizontalOffset));
            Popup.VerticalOffset = Math.Max(bounds.Top,
                                            Math.Min(bounds.Bottom - ResizedElement.Height, Popup.VerticalOffset));
        }
    }
}
