namespace Raven.ManagementStudio.UI.Silverlight.Controls
{
    using System.Windows;
    using System.Windows.Controls;

    public class DocumentsContainer : ItemsControl
    {
        public static readonly DependencyProperty PreviewVisibilityProperty =
            DependencyProperty.Register("PreviewVisibility", typeof(Visibility), typeof(DocumentsContainer), new PropertyMetadata(Visibility.Collapsed));

        public Visibility PreviewVisibility
        {
            get { return (Visibility)GetValue(PreviewVisibilityProperty); }

            set { SetValue(PreviewVisibilityProperty, value); }
        }
    }
}
