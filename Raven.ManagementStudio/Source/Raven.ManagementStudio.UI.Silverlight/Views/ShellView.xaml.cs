using System.Windows;
using System.Windows.Controls;

namespace Raven.ManagementStudio.UI.Silverlight.Views
{
    public partial class ShellView : UserControl
    {
        public ShellView()
        {
            InitializeComponent();
        }

        private void TitleBarMouseLeftButtonDown(object sender, System.Windows.Input.MouseButtonEventArgs e)
        {
            Application.Current.MainWindow.DragMove();
        }
    }
}
