using System.Windows;
using System.Windows.Input;
using System.Windows.Interactivity;
using System.ComponentModel;
namespace Raven.Studio.Behaviors
{
    /// <summary>
    /// Class for resizing VisualElements.
    /// </summary>
    /// <remarks>
    ///  // ResizeDragBehavior Created by Andries van der Meulen. http://resizedragbehavior.codeplex.com/
    /// </remarks>
    public class ResizeDragBehavior : Behavior<FrameworkElement>
    {
        /// <summary>
        /// The available directions where the element can resize to.
        /// </summary>
        public enum ResizeDirections
        {
            None,
            Right,
            Left,
            Up,
            Down,
            LeftUp,
            RightUp,
            LeftDown,
            RightDown,
        }

        /// <summary>
        /// Flag to see if resizing is in progress.
        /// </summary>
        private bool _IsResizing = false;

        /// <summary>
        /// Point to remember the mouse movement change.
        /// </summary>
        private Point _InitialResizePoint;

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
        /// The elementname which has to resize.
        /// </summary>
        [Category("Common Properties"), Description("The targeted element that must be resized."),
         CustomPropertyValueEditorAttribute(CustomPropertyValueEditor.Element)]
        public string ResizeElementName
        {
            get { return (string) GetValue(ResizeElementNameProperty); }
            set { SetValue(ResizeElementNameProperty, value); }
        }

        // Using a DependencyProperty as the backing store for ResizeElement.  This enables animation, styling, binding, etc...
        public static readonly DependencyProperty ResizeElementNameProperty =
            DependencyProperty.Register("ResizeElementName", typeof (string), typeof (ResizeDragBehavior),
                                        new PropertyMetadata(string.Empty, ResizeElementNameChanged));

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
        /// The element which has to resize.
        /// </summary>
        private FrameworkElement ResizeElement { get; set; }

        /// <summary>
        /// The direction the resizing must take place.
        /// </summary>
        [Category("Common Properties"), Description("The direction the resizing must take place.")]
        public ResizeDirections ResizeDirection
        {
            get { return (ResizeDirections) GetValue(ResizeDirectionProperty); }
            set { SetValue(ResizeDirectionProperty, value); }
        }

        // Using a DependencyProperty as the backing store for ResizeDirection.  This enables animation, styling, binding, etc...
        public static readonly DependencyProperty ResizeDirectionProperty =
            DependencyProperty.Register("ResizeDirection", typeof (ResizeDirections), typeof (ResizeDragBehavior),
                                        new PropertyMetadata(ResizeDirections.None, ResizeDirectionChanged));

        /// <summary>
        /// Sets the mouse cursor.
        /// </summary>
        private void SetResizeDirections()
        {
            if (DragElement == null) return;

            switch (ResizeDirection)
            {
                case ResizeDirections.Left:
                case ResizeDirections.Right:
                    DragElement.Cursor = Cursors.SizeWE;
                    break;
                case ResizeDirections.Up:
                case ResizeDirections.Down:
                    DragElement.Cursor = Cursors.SizeNS;
                    break;
                case ResizeDirections.LeftUp:
                case ResizeDirections.LeftDown:
                case ResizeDirections.RightDown:
                case ResizeDirections.RightUp:
                    DragElement.Cursor = Cursors.SizeNWSE;
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
        /// Event when the selected resize direction changed.
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private static void ResizeDirectionChanged(DependencyObject sender, DependencyPropertyChangedEventArgs e)
        {
            ((ResizeDragBehavior) sender).SetResizeDirections();
        }

        /// <summary>
        /// Event when the selected resize elementname changed.
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private static void ResizeElementNameChanged(DependencyObject sender, DependencyPropertyChangedEventArgs e)
        {
            ResizeDragBehavior behavior = sender as ResizeDragBehavior;
            if (behavior.DragElement != null)
                behavior.ResizeElement = behavior.DragElement.FindName(behavior.ResizeElementName) as FrameworkElement;
        }

        /// <summary>
        /// Event when the left mouse button is down on the dragging element.
        /// Starts resizing.
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void DragElementMouseLeftButtonDown(object sender, MouseButtonEventArgs e)
        {
            if (ResizeElement == null)
                ResizeElement = DragElement.FindName(ResizeElementName) as FrameworkElement;

            if (CanResize)
            {
                // Capture the mouse
                ((FrameworkElement) sender).CaptureMouse();

                // Store the start position
                _InitialResizePoint = e.GetPosition(ResizeElement.Parent as UIElement);
                _ResizeElementWindowSize.Width = (!double.IsNaN(ResizeElement.Width)
                                                      ? ResizeElement.Width
                                                      : ResizeElement.ActualWidth);
                _ResizeElementWindowSize.Height = (!double.IsNaN(ResizeElement.Height)
                                                       ? ResizeElement.Height
                                                       : ResizeElement.ActualHeight);

                // Set resizing to true
                _IsResizing = true;
            }
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
                ((FrameworkElement) sender).ReleaseMouseCapture();

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

            resize(e.GetPosition(ResizeElement.Parent as UIElement));

            _InitialResizePoint = e.GetPosition(ResizeElement.Parent as UIElement);
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
                if (ResizeDirection == ResizeDirections.None)
                    return false;
                if (DragElement == null)
                    return false;
                if (ResizeElementName == null)
                    return false;

                return true;
            }
        }

        /// <summary>
        /// Calculates the distance between the two points and resizes the resizeelement.
        /// </summary>
        /// <param name="mousePosition">Point referenced to element.</param>
        private void resize(Point mousePosition)
        {
            if (ResizeElement == null)
                return;

            if (_InitialResizePoint == null)
                return;

            double deltaX = mousePosition.X - _InitialResizePoint.X;
            double deltaY = mousePosition.Y - _InitialResizePoint.Y;

            switch (ResizeDirection)
            {
                case ResizeDirections.Right:
                    if (ResizeElement.Width - deltaX > 0)
                        ResizeElement.Width -= deltaX;
                    break;
                case ResizeDirections.Left:
                    if (ResizeElement.Width + deltaX > 0)
                        ResizeElement.Width += deltaX;
                    break;
                case ResizeDirections.Up:
                    if (ResizeElement.Height + deltaY > 0)
                        ResizeElement.Height += deltaY;
                    break;
                case ResizeDirections.Down:
                    if (ResizeElement.Height - deltaY > 0)
                        ResizeElement.Height -= deltaY;
                    break;
                case ResizeDirections.LeftUp:
                    if (ResizeElement.Height + deltaY > 0)
                        ResizeElement.Height += deltaY;
                    if (ResizeElement.Width + deltaX > 0)
                        ResizeElement.Width += deltaX;
                    break;
                case ResizeDirections.RightUp:
                    if (ResizeElement.Width - deltaX > 0)
                        ResizeElement.Width -= deltaX;
                    if (ResizeElement.Height + deltaY > 0)
                        ResizeElement.Height += deltaY;
                    break;
                case ResizeDirections.LeftDown:
                    if (ResizeElement.Width + deltaX > 0)
                        ResizeElement.Width += deltaX;
                    if (ResizeElement.Height - deltaY > 0)
                        ResizeElement.Height -= deltaY;
                    break;
                case ResizeDirections.RightDown:
                    if (ResizeElement.Width - deltaX > 0)
                        ResizeElement.Width -= deltaX;
                    if (ResizeElement.Height - deltaY > 0)
                        ResizeElement.Height -= deltaY;
                    break;
            }
        }

    }
}

