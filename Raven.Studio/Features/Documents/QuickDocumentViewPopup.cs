using System;
using System.Net;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Documents;
using System.Windows.Ink;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Animation;
using System.Windows.Shapes;
using System.Windows.Threading;
using ActiproSoftware.Compatibility;
using Boogaart.Silverlight.Behaviors;
using Raven.Studio.Features.JsonEditor;
using Raven.Studio.Infrastructure;
using Popup = System.Windows.Controls.Primitives.Popup;

namespace Raven.Studio.Features.Documents
{
    public partial class QuickDocumentViewPopup : ContentControl
    {

        public QuickDocumentViewPopup()
        {
            DefaultStyleKey = typeof(QuickDocumentViewPopup);

            MouseEnter += HandleMouseEnter;
            MouseLeave += HandleMouseLeave;
            Unloaded += HandleUnloaded;
            MouseLeftButtonDown += HandleMouseClick;
        }

        public string DocumentId
        {
            get { return (string)GetValue(DocumentIdProperty); }
            set { SetValue(DocumentIdProperty, value); }
        }

        public static readonly DependencyProperty DocumentIdProperty =
            DependencyProperty.Register("DocumentId", typeof(string), typeof(QuickDocumentViewPopup), new PropertyMetadata(null));

        private DispatcherTimer timer;
        private Popup popup;
        private Border contentHost;

        private void HandleMouseClick(object sender, MouseButtonEventArgs e)
        {
            if (string.IsNullOrEmpty(DocumentId) || (Keyboard.Modifiers & ModifierKeys.Shift) != ModifierKeys.Shift)
                return;

            UrlUtil.Navigate("/Edit?id=" + DocumentId);

            e.Handled = true;
        }


        public override void OnApplyTemplate()
        {
            popup = GetTemplateChild("PART_Popup") as Popup;
            contentHost = GetTemplateChild("PART_ContentHost") as Border;
        }

        private void HandleMouseLeave(object sender, MouseEventArgs e)
        {
            StopTimer();
        }

        private void HandleUnloaded(object sender, RoutedEventArgs e)
        {
            StopTimer();
            CloseToolTip();
        }

        private void CloseToolTip()
        {
            if (popup != null)
            {
                popup.IsOpen = false;
                contentHost.Child = null;
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
            if (string.IsNullOrEmpty(DocumentId))
            {
                return;
            }

            timer = new DispatcherTimer() { Interval = TimeSpan.FromMilliseconds(500) };
            timer.Tick += HandleTimerTick;
            timer.Start();
        }

        private void HandleTimerTick(object sender, EventArgs e)
        {
            StopTimer();

            contentHost.Child = new QuickDocumentView {DocumentId = DocumentId};

            popup.IsOpen = true;
        }
    }
}
