namespace Raven.ManagementStudio.UI.Silverlight.Plugins.Common
{
    using System.Windows;
    using System.Windows.Controls;

    public partial class DocumentView : UserControl
    {
        public DocumentView()
        {
            InitializeComponent();
            VisualStateManager.GoToState(this, "NormalState", true);
        }
    }
}
