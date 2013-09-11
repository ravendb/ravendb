using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Net;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Controls.Primitives;
using System.Windows.Data;
using System.Windows.Documents;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Animation;
using System.Windows.Shapes;
using Raven.Studio.Behaviors;
using Raven.Studio.Models;

namespace Raven.Studio.Features.Documents
{
    public partial class DocumentPadLayer : UserControl
    {
        private DocumentPadView view;
        private Popup popup;

        public DocumentPadLayer()
        {
            InitializeComponent();

            Loaded += HandleLoaded;
            SizeChanged += HandleSizeChanged;
        }

        private void HandleSizeChanged(object sender, SizeChangedEventArgs e)
        {
            if (view != null)
            {
                var bounds = GetCurrentPopupBounds();
                ResizablePopupFrameBehavior.SetResizeBounds(view, bounds);
                RepositionPopup(bounds, e.PreviousSize, e.NewSize);
            }
        }

        private void RepositionPopup(Rect currentBounds, Size previousSize, Size newSize)
        {
            var previousBounds = GetPopupBounds(previousSize);

            popup.HorizontalOffset = Math.Max(Math.Min(currentBounds.Right - view.Width, popup.HorizontalOffset), currentBounds.Left);
            popup.VerticalOffset = Math.Max(Math.Min(currentBounds.Bottom - view.Height, popup.VerticalOffset), currentBounds.Top);

            // if the popup is right up against the right or bottom edges, make sure it stays there as the window is resized
            if (newSize.Width > previousSize.Width)
            {
                if (Math.Abs((popup.HorizontalOffset + view.Width) - previousBounds.Right) < 2)
                {
                    popup.HorizontalOffset += (newSize.Width - previousSize.Width);
                }
            }

            if (newSize.Height > previousSize.Height)
            {
                if (Math.Abs((popup.VerticalOffset + view.Height) - previousBounds.Bottom) < 2)
                {
                    popup.VerticalOffset += (newSize.Height - previousSize.Height);
                }
            }
        }

        private void HandleLoaded(object sender, RoutedEventArgs e)
        {
            popup = new Popup();

            view = new DocumentPadView() {DataContext = ApplicationModel.Current.DocumentPad};

            ResizablePopupFrameBehavior.SetParentPopup(view, popup);
            ResizablePopupFrameBehavior.SetResizeBounds(view, GetCurrentPopupBounds());

            view.Width = 300;
            view.Height = 300;

            PropertyChangedEventHandler documentPadOnPropertyChanged = null;
            documentPadOnPropertyChanged = (s, args) =>
            {
                ApplicationModel.Current.DocumentPad.PropertyChanged -= documentPadOnPropertyChanged;
                var bounds = GetCurrentPopupBounds();
                if (args.PropertyName == "IsOpen")
                {
                    popup.HorizontalOffset = bounds.Right - view.Width;
                    popup.VerticalOffset = bounds.Bottom - view.Height;
                }
            };

            ApplicationModel.Current.DocumentPad.PropertyChanged += documentPadOnPropertyChanged;
            

            popup.Child = view;
            popup.SetBinding(Popup.IsOpenProperty, new Binding("IsOpen") {Source = ApplicationModel.Current.DocumentPad});
        }

        private Rect GetCurrentPopupBounds()
        {
            return GetPopupBounds(RenderSize);
        }

        private Rect GetPopupBounds(Size size)
        {
            return TransformToVisual(Application.Current.RootVisual).TransformBounds(new Rect(new Point(), size));
        }
    }
}
