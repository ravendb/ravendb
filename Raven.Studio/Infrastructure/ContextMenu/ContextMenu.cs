// (c) Copyright Microsoft Corporation.
// This source is subject to the Microsoft Public License (Ms-PL).
// Please see http://go.microsoft.com/fwlink/?LinkID=131993 for details.
// All other rights reserved.

using System.ComponentModel;
using System.Windows.Controls.Primitives;
using System.Windows.Data;
using System.Windows.Input;
using System.Windows.Media;
using Raven.Studio.Infrastructure;

namespace System.Windows.Controls
{
    /// <summary>
    /// Represents a pop-up menu that enables a control to expose functionality that is specific to the context of the control.
    /// </summary>
    /// <QualityBand>Preview</QualityBand>
    public class ContextMenu : MenuBase
    {
        /// <summary>
        /// Stores a reference to the current root visual.
        /// </summary>
        private FrameworkElement _rootVisual;

        /// <summary>
        /// Stores the last known mouse position (via MouseMove).
        /// </summary>
        private Point _mousePosition;

        /// <summary>
        /// Stores a reference to the object that owns the ContextMenu.
        /// </summary>
        private DependencyObject _owner;

        /// <summary>
        /// Stores a reference to the current Popup.
        /// </summary>
        private Popup _popup;

        /// <summary>
        /// Stores a reference to the current overlay.
        /// </summary>
        private Panel _overlay;

        /// <summary>
        /// Stores a reference to the current Popup alignment point.
        /// </summary>
        private Point _popupAlignmentPoint;

        /// <summary>
        /// Stores a value indicating whether the IsOpen property is being updated by ContextMenu.
        /// </summary>
        private bool _settingIsOpen;

        /// <summary>
        /// Gets or sets the owning object for the ContextMenu.
        /// </summary>
        public DependencyObject Owner
        {
            get { return _owner; }
            set
            {
                if (null != _owner)
                {
                    FrameworkElement ownerFrameworkElement = _owner as FrameworkElement;
                    if (null != ownerFrameworkElement)
                    {
                        ownerFrameworkElement.MouseRightButtonDown -= new MouseButtonEventHandler(HandleOwnerMouseRightButtonDown);
                    }
                }
                _owner = value;
                if (null != _owner)
                {
                    FrameworkElement ownerFrameworkElement = _owner as FrameworkElement;
                    if (null != ownerFrameworkElement)
                    {
                        ownerFrameworkElement.MouseRightButtonDown += new MouseButtonEventHandler(HandleOwnerMouseRightButtonDown);
                    }
                }
            }
        }

        /// <summary>
        /// Gets or sets the horizontal distance between the target origin and the popup alignment point.
        /// </summary>
        public double HorizontalOffset
        {
            get { return (double)GetValue(HorizontalOffsetProperty); }
            set { SetValue(HorizontalOffsetProperty, value); }
        }

        /// <summary>
        /// Identifies the HorizontalOffset dependency property.
        /// </summary>
        public static readonly DependencyProperty HorizontalOffsetProperty = DependencyProperty.Register(
            "HorizontalOffset",
            typeof(double),
            typeof(ContextMenu),
            new PropertyMetadata(0.0, OnHorizontalVerticalOffsetChanged));

        /// <summary>
        /// Gets or sets the vertical distance between the target origin and the popup alignment point.
        /// </summary>
        public double VerticalOffset
        {
            get { return (double)GetValue(VerticalOffsetProperty); }
            set { SetValue(VerticalOffsetProperty, value); }
        }

        /// <summary>
        /// Identifies the VerticalOffset dependency property.
        /// </summary>
        public static readonly DependencyProperty VerticalOffsetProperty = DependencyProperty.Register(
            "VerticalOffset",
            typeof(double),
            typeof(ContextMenu),
            new PropertyMetadata(0.0, OnHorizontalVerticalOffsetChanged));

        /// <summary>
        /// Handles changes to the HorizontalOffset or VerticalOffset DependencyProperty.
        /// </summary>
        /// <param name="o">DependencyObject that changed.</param>
        /// <param name="e">Event data for the DependencyPropertyChangedEvent.</param>
        private static void OnHorizontalVerticalOffsetChanged(DependencyObject o, DependencyPropertyChangedEventArgs e)
        {
            ((ContextMenu)o).UpdateContextMenuPlacement();
        }

        /// <summary>
        /// Gets or sets a value indicating whether the ContextMenu is visible.
        /// </summary>
        public bool IsOpen
        {
            get { return (bool)GetValue(IsOpenProperty); }
            set { SetValue(IsOpenProperty, value); }
        }

        /// <summary>
        /// Identifies the IsOpen dependency property.
        /// </summary>
        public static readonly DependencyProperty IsOpenProperty = DependencyProperty.Register(
            "IsOpen",
            typeof(bool),
            typeof(ContextMenu),
            new PropertyMetadata(false, OnIsOpenChanged));

        /// <summary>
        /// Handles changes to the IsOpen DependencyProperty.
        /// </summary>
        /// <param name="o">DependencyObject that changed.</param>
        /// <param name="e">Event data for the DependencyPropertyChangedEvent.</param>
        private static void OnIsOpenChanged(DependencyObject o, DependencyPropertyChangedEventArgs e)
        {
            ((ContextMenu)o).OnIsOpenChanged((bool)e.OldValue, (bool)e.NewValue);
        }

        /// <summary>
        /// Handles changes to the IsOpen property.
        /// </summary>
        /// <param name="oldValue">Old value.</param>
        /// <param name="newValue">New value.</param>
        private void OnIsOpenChanged(bool oldValue, bool newValue)
        {
            if (!_settingIsOpen)
            {
                if (newValue)
                {
                    OpenPopup(_mousePosition);
                }
                else
                {
                    ClosePopup();
                }
            }
        }

        /// <summary>
        /// Occurs when a particular instance of a ContextMenu opens.
        /// </summary>
        public event RoutedEventHandler Opened;

        /// <summary>
        /// Called when the Opened event occurs.
        /// </summary>
        /// <param name="e">Event arguments.</param>
        protected virtual void OnOpened(RoutedEventArgs e)
        {
            RoutedEventHandler handler = Opened;
            if (null != handler)
            {
                handler.Invoke(this, e);
            }
        }

        /// <summary>
        /// Occurs when a particular instance of a ContextMenu closes.
        /// </summary>
        public event RoutedEventHandler Closed;

        /// <summary>
        /// Called when the Closed event occurs.
        /// </summary>
        /// <param name="e">Event arguments.</param>
        protected virtual void OnClosed(RoutedEventArgs e)
        {
            RoutedEventHandler handler = Closed;
            if (null != handler)
            {
                handler.Invoke(this, e);
            }
        }

        /// <summary>
        /// Initializes a new instance of the ContextMenu class.
        /// </summary>
        public ContextMenu()
        {
            DefaultStyleKey = typeof(ContextMenu);

            // Temporarily hook LayoutUpdated to find out when Application.Current.RootVisual gets set.
            LayoutUpdated += new EventHandler(HandleLayoutUpdated);
        }

        /// <summary>
        /// Called when the left mouse button is pressed.
        /// </summary>
        /// <param name="e">The event data for the MouseLeftButtonDown event.</param>
        protected override void OnMouseLeftButtonDown(MouseButtonEventArgs e)
        {
            e.Handled = true;
            base.OnMouseLeftButtonDown(e);
        }

        /// <summary>
        /// Called when the right mouse button is pressed.
        /// </summary>
        /// <param name="e">The event data for the MouseRightButtonDown event.</param>
        protected override void OnMouseRightButtonDown(MouseButtonEventArgs e)
        {
            e.Handled = true;
            base.OnMouseRightButtonDown(e);
        }

        /// <summary>
        /// Responds to the KeyDown event.
        /// </summary>
        /// <param name="e">The event data for the KeyDown event.</param>
        protected override void OnKeyDown(KeyEventArgs e)
        {
            switch (e.Key)
            {
                case Key.Up:
                    FocusNextItem(false);
                    e.Handled = true;
                    break;
                case Key.Down:
                    FocusNextItem(true);
                    e.Handled = true;
                    break;
                case Key.Escape:
                    ClosePopup();
                    e.Handled = true;
                    break;
                // case Key.Apps: // Key.Apps not defined by Silverlight 4
            }
            base.OnKeyDown(e);
        }

        /// <summary>
        /// Handles the LayoutUpdated event to capture Application.Current.RootVisual.
        /// </summary>
        /// <param name="sender">Source of the event.</param>
        /// <param name="e">Event arguments.</param>
        private void HandleLayoutUpdated(object sender, EventArgs e)
        {
            if (null != Application.Current.RootVisual)
            {
                // Application.Current.RootVisual is valid
                InitializeRootVisual();
                // Unhook event
                LayoutUpdated -= new EventHandler(HandleLayoutUpdated);
            }
        }

        /// <summary>
        /// Handles the RootVisual's MouseMove event to track the last mouse position.
        /// </summary>
        /// <param name="sender">Source of the event.</param>
        /// <param name="e">Event arguments.</param>
        private void HandleRootVisualMouseMove(object sender, MouseEventArgs e)
        {
            _mousePosition = e.GetPosition(null);
        }

        /// <summary>
        /// Handles the MouseRightButtonDown event for the owning element.
        /// </summary>
        /// <param name="sender">Source of the event.</param>
        /// <param name="e">Event arguments.</param>
        private void HandleOwnerMouseRightButtonDown(object sender, MouseButtonEventArgs e)
        {
            OpenPopup(e.GetPosition(null));
            e.Handled = true;
        }

        /// <summary>
        /// Initialize the _rootVisual property (if possible and not already done).
        /// </summary>
        private void InitializeRootVisual()
        {
            if (null == _rootVisual)
            {
                // Try to capture the Application's RootVisual
                _rootVisual = Application.Current.RootVisual as FrameworkElement;
                if (null != _rootVisual)
                {
                    var rootVisual = _rootVisual;
                    // Use a weak event listener.
                    var rootVisualMouseMoveListener = new WeakEventListener<ContextMenu, object, MouseEventArgs>(this)
                    {
                        OnEventAction =
                            (instance, source, eventArgs) =>
                            instance.HandleRootVisualMouseMove(source, eventArgs),
                        OnDetachAction =
                            (weakEventListener) =>
                            rootVisual.MouseMove -= weakEventListener.OnEvent
                    };

                    rootVisual.MouseMove += rootVisualMouseMoveListener.OnEvent;
                }
            }
        }

        /// <summary>
        /// Sets focus to the next item in the ContextMenu.
        /// </summary>
        /// <param name="down">True to move the focus down; false to move it up.</param>
        private void FocusNextItem(bool down)
        {
            int count = Items.Count;
            int startingIndex = down ? -1 : count;
            MenuItem focusedMenuItem = FocusManager.GetFocusedElement() as MenuItem;
            if (null != focusedMenuItem && (this == focusedMenuItem.ParentMenuBase))
            {
                startingIndex = ItemContainerGenerator.IndexFromContainer(focusedMenuItem);
            }
            int index = startingIndex;
            do
            {
                index = (index + count + (down ? 1 : -1)) % count;
                MenuItem container = ItemContainerGenerator.ContainerFromIndex(index) as MenuItem;
                if (null != container)
                {
                    if (container.IsEnabled && container.Focus())
                    {
                        break;
                    }
                }
            }
            while (index != startingIndex);
        }

        /// <summary>
        /// Called when a child MenuItem is clicked.
        /// </summary>
        internal void ChildMenuItemClicked()
        {
            ClosePopup();
        }

        /// <summary>
        /// Handles the SizeChanged event for the ContextMenu or RootVisual.
        /// </summary>
        /// <param name="sender">Source of the event.</param>
        /// <param name="e">Event arguments.</param>
        private void HandleContextMenuOrRootVisualSizeChanged(object sender, SizeChangedEventArgs e)
        {
            UpdateContextMenuPlacement();
        }

        /// <summary>
        /// Handles the MouseButtonDown events for the overlay.
        /// </summary>
        /// <param name="sender">Source of the event.</param>
        /// <param name="e">Event arguments.</param>
        private void HandleOverlayMouseButtonDown(object sender, MouseButtonEventArgs e)
        {
            ClosePopup();
            e.Handled = true;
        }

        /// <summary>
        /// Updates the location and size of the Popup and overlay.
        /// </summary>
        private void UpdateContextMenuPlacement()
        {
            if ((null != _rootVisual) && (null != _overlay))
            {
                // Start with the current Popup alignment point
                double x = _popupAlignmentPoint.X;
                double y = _popupAlignmentPoint.Y;
                // Adjust for offset
                x += HorizontalOffset;
                y += VerticalOffset;
                // Try not to let it stick out too far to the right/bottom
                x = Math.Min(x, _rootVisual.ActualWidth - ActualWidth);
                y = Math.Min(y, _rootVisual.ActualHeight - ActualHeight);
                // Do not let it stick out too far to the left/top
                x = Math.Max(x, 0);
                y = Math.Max(y, 0);
                // Set the new location
                Canvas.SetLeft(this, x);
                Canvas.SetTop(this, y);
                // Size the overlay to match the new container
                _overlay.Width = _rootVisual.ActualWidth;
                _overlay.Height = _rootVisual.ActualHeight;
            }
        }

        /// <summary>
        /// Opens the Popup.
        /// </summary>
        /// <param name="position">Position to place the Popup.</param>
        private void OpenPopup(Point position)
        {
            _popupAlignmentPoint = position;

            InitializeRootVisual();

            _overlay = new Canvas { Background = new SolidColorBrush(Colors.Transparent) };
            _overlay.MouseLeftButtonDown += new MouseButtonEventHandler(HandleOverlayMouseButtonDown);
            _overlay.MouseRightButtonDown += new MouseButtonEventHandler(HandleOverlayMouseButtonDown);
            _overlay.Children.Add(this);

            _popup = new Popup { Child = _overlay };

            SizeChanged += new SizeChangedEventHandler(HandleContextMenuOrRootVisualSizeChanged);
            if (null != _rootVisual)
            {
                _rootVisual.SizeChanged += new SizeChangedEventHandler(HandleContextMenuOrRootVisualSizeChanged);
            }
            UpdateContextMenuPlacement();

            if (ReadLocalValue(DataContextProperty) == DependencyProperty.UnsetValue)
            {
                DependencyObject dataContextSource = Owner ?? _rootVisual;
                SetBinding(DataContextProperty, new Binding("DataContext") { Source = dataContextSource });
            }

            _popup.IsOpen = true;
            Focus();

            // Update IsOpen
            _settingIsOpen = true;
            IsOpen = true;
            _settingIsOpen = false;

            OnOpened(new RoutedEventArgs());
        }

        /// <summary>
        /// Closes the Popup.
        /// </summary>
        private void ClosePopup()
        {
            if (null != _popup)
            {
                _popup.IsOpen = false;
                _popup.Child = null;
                _popup = null;
            }
            if (null != _overlay)
            {
                _overlay.Children.Clear();
                _overlay = null;
            }
            SizeChanged -= new SizeChangedEventHandler(HandleContextMenuOrRootVisualSizeChanged);
            if (null != _rootVisual)
            {
                _rootVisual.SizeChanged -= new SizeChangedEventHandler(HandleContextMenuOrRootVisualSizeChanged);
            }

            // Update IsOpen
            _settingIsOpen = true;
            IsOpen = false;
            _settingIsOpen = false;

            OnClosed(new RoutedEventArgs());
        }
    }
}
