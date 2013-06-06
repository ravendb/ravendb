using System.Windows;

namespace Raven.Studio.Features.Documents
{
    public partial class DocumentsView
    {
        public DocumentsView()
        {
            InitializeComponent();
        }

        public bool ShowHeader
        {
            get { return (bool)GetValue(ShowHeaderProperty); }
            set { SetValue(ShowHeaderProperty, value); }
        }

        public static readonly DependencyProperty ShowHeaderProperty =
            DependencyProperty.Register("ShowHeader", typeof(bool), typeof(DocumentsView), new PropertyMetadata(true));

    }
}