namespace Raven.ManagementStudio
{
    using System.Windows;

    public partial class App : Application
    {
        AppBootstrapper bootstrapper;

        public App()
        {
            bootstrapper = new AppBootstrapper();

            InitializeComponent();
        }
    }
}
