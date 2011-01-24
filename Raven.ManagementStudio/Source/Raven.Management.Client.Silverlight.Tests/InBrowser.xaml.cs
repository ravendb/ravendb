namespace Raven.Management.Client.Silverlight.Tests
{
    using System.Windows;
    using System.Windows.Controls;
    using System.Windows.Navigation;

    public partial class InBrowser : Page
    {
        public InBrowser()
        {
            InitializeComponent();

            Loaded += InBrowser_Loaded;
        }

        private void InBrowser_Loaded(object sender, RoutedEventArgs e)
        {
            if (!Application.Current.IsRunningOutOfBrowser && Application.Current.InstallState == InstallState.Installed)
            {
                label1.Visibility = Visibility.Visible;
                BtnInstall.Visibility = Visibility.Collapsed;
            }
            else
            {
                label1.Visibility = Visibility.Collapsed;
                BtnInstall.Visibility = Visibility.Visible;
            }
        }

        // Executes when the user navigates to this page.
        protected override void OnNavigatedTo(NavigationEventArgs e)
        {
        }

        private void BtnInstall_Click(object sender, RoutedEventArgs e)
        {
            if (!Application.Current.IsRunningOutOfBrowser)
            {
                bool install = Application.Current.Install();
            }
        }
    }
}