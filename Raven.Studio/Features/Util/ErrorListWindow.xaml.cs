using System.Windows.Input;
using Raven.Studio.Infrastructure;
using Raven.Studio.Messages;
using Raven.Studio.Models;

namespace Raven.Studio.Features.Util
{
    public partial class ErrorListWindow : DialogView
    {
        public ErrorListWindow()
        {
            InitializeComponent();

            KeyUp += HandleKeyUp;
        }

        private void HandleKeyUp(object sender, KeyEventArgs e)
        {
            if (e.Key == Key.Escape)
            {
                Close();
            }
        }

        public static void ShowErrors(Notification selectedNotification = null)
        {
            var window = new ErrorListWindow();
            window.DataContext = new StudioErrorListModel() { SelectedItem = selectedNotification};

            window.Show();
        }
    }
}