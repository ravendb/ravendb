using System;
using System.Net;
using System.Threading.Tasks;
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
using System.Windows.Threading;
using Raven.Studio.Extensions;
using Raven.Studio.Features.Documents;
using Raven.Studio.Features.JsonEditor;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Behaviors
{
    public class ShowQuickDocumentPopupBehavior : Behavior<FrameworkElement>
    {
        private const int PotentialHeight = 200;
        private const int PotentialWidth = 400;

        private static event EventHandler<EventArgs> QuickDocumentViewOpening;

        private static void OnQuickDocumentViewOpening()
        {
            var handler = QuickDocumentViewOpening;
            if (handler != null) handler(null, EventArgs.Empty);
        }

        protected override void OnAttached()
        {
            base.OnAttached();

            AssociatedObject.MouseEnter += HandleMouseEnter;
            AssociatedObject.MouseLeave += HandleMouseLeave;
            AssociatedObject.Unloaded += HandleUnloaded;
            AssociatedObject.MouseLeftButtonDown += HandleMouseClick;
        }

        protected override void OnDetaching()
        {
            base.OnDetaching();

            AssociatedObject.MouseEnter -= HandleMouseEnter;
            AssociatedObject.MouseLeave -= HandleMouseLeave;
            AssociatedObject.Unloaded -= HandleUnloaded;
            AssociatedObject.MouseLeftButtonDown -= HandleMouseClick;
        }

        public string DocumentId
        {
            get { return (string)GetValue(DocumentIdProperty); }
            set { SetValue(DocumentIdProperty, value); }
        }

        public static readonly DependencyProperty DocumentIdProperty =
            DependencyProperty.Register("DocumentId", typeof(string), typeof(ShowQuickDocumentPopupBehavior), new PropertyMetadata(null));

        public string PotentialDocumentId
        {
            get { return (string)GetValue(PotentialDocumentIdProperty); }
            set { SetValue(PotentialDocumentIdProperty, value); }
        }

        public static readonly DependencyProperty PotentialDocumentIdProperty =
            DependencyProperty.Register("PotentialDocumentId", typeof(string), typeof(ShowQuickDocumentPopupBehavior), new PropertyMetadata(null));

        
        private DispatcherTimer timer;
        private Popup popup;
        private FrameworkElement rootVisual;

        private void HandleMouseClick(object sender, MouseButtonEventArgs e)
        {
            if (string.IsNullOrEmpty(DocumentId) || (Keyboard.Modifiers & ModifierKeys.Shift) != ModifierKeys.Shift)
                return;

            UrlUtil.Navigate("/Edit?id=" + DocumentId);

            e.Handled = true;
        }

        private void HandlePopupClosed(object sender, EventArgs e)
        {
            ClosePopup();
        }

        private void HandleMouseLeave(object sender, MouseEventArgs e)
        {
            StopTimer();
        }

        private void HandleUnloaded(object sender, RoutedEventArgs e)
        {
            StopTimer();
            ClosePopup();
        }

        private void ClosePopup()
        {
            if (popup != null)
            {
                popup.IsOpen = false;

                QuickDocumentViewOpening -= HandleQuickDocumentViewOpening;

                if (rootVisual != null)
                {
                    rootVisual.MouseMove -= HandleRootMouseMove;
                    rootVisual = null;
                }

                popup = null;
            }
        }

        private void StopTimer()
        {
            if (timer != null)
            {
                timer.Stop();
                timer = null;
            }
        }

        private void HandleMouseEnter(object sender, MouseEventArgs e)
        {
            if (string.IsNullOrEmpty(DocumentId) && string.IsNullOrEmpty(PotentialDocumentId))
            {
                return;
            }

            timer = new DispatcherTimer() { Interval = TimeSpan.FromMilliseconds(500) };
            timer.Tick += HandleTimerTick;
            timer.Start();
        }

        private async void HandleTimerTick(object sender, EventArgs e)
        {
            StopTimer();

            if (popup == null)
            {
                var documentId = await GetDocumentId();
                if (string.IsNullOrEmpty(documentId))
                {
                    return;
                }

                var quickDocumentView = new QuickDocumentView { DocumentId = documentId, Margin = new Thickness(2) };
                quickDocumentView.DocumentShown += delegate { PositionPopup(); };

                popup = new Popup()
                {
                    Child = new Border()
                    {
                        Background = new SolidColorBrush(Color.FromArgb(255, 230, 231, 235)),
                        BorderBrush = new SolidColorBrush(Color.FromArgb(255, 203, 205, 218)),
                        BorderThickness = new Thickness(1),
                        Child = quickDocumentView
                    }
                };

                Boogaart.Silverlight.Behaviors.Popup.SetStaysOpen(popup, false);

                popup.IsOpen = true;

                rootVisual = VisualTreeHelper.GetRoot(AssociatedObject) as FrameworkElement;
                rootVisual.MouseMove += HandleRootMouseMove;

                Dispatcher.BeginInvoke(PositionPopup);

                OnQuickDocumentViewOpening();

                QuickDocumentViewOpening += HandleQuickDocumentViewOpening;
            }
        }

        private async Task<string> GetDocumentId()
        {
            if (!string.IsNullOrEmpty(DocumentId))
            {
                return DocumentId;
            }

            if (!DocumentIdCheckHelpers.IsPotentialId(PotentialDocumentId))
            {
                return null;
            }

            try
            {
                var ids = await DocumentIdCheckHelpers.GetActualIds(new[] {PotentialDocumentId});
                if (ids.Count == 1)
                {
                    return PotentialDocumentId;
                }
            }
            catch
            {
                // not bothered about exceptions here
            }

            return null;
        }

        private void PositionPopup()
        {
            if (popup == null)
            {
                return;
            }

            popup.UpdateLayout();

            var targetBounds = new Rect(new Point(0, 0), AssociatedObject.RenderSize);
            targetBounds = AssociatedObject.TransformToVisual(rootVisual).TransformBounds(targetBounds);

            var popupSize = popup.Child.RenderSize;

            if (targetBounds.Bottom + PotentialHeight <= rootVisual.ActualHeight)
            {
                popup.VerticalOffset = targetBounds.Bottom;
            }
            else
            {
                popup.VerticalOffset = targetBounds.Top - popupSize.Height;
            }

            if (targetBounds.Left + PotentialWidth <= rootVisual.ActualWidth)
            {
                popup.HorizontalOffset = targetBounds.Left;
            }
            else
            {
                popup.HorizontalOffset = targetBounds.Right - popupSize.Width;
            }
        }

        private void HandleRootMouseMove(object sender, MouseEventArgs e)
        {
            var bounds = new Rect(new Point(0,0), popup.Child.RenderSize);
            bounds = bounds.Inflate(40);

            var position = e.GetPosition(popup);
            if (!bounds.Contains(position))
            {
                ClosePopup();
            }
        }

        private void HandleQuickDocumentViewOpening(object sender, EventArgs e)
        {
            if (popup.IsOpen)
            {
                ClosePopup();
            }
        }
    }
}
