namespace Raven.ManagementStudio.UI.Silverlight
{
    using System.Windows;

    public partial class App : Application
    {
        private AppBootstrapper bootstrapper;

        public App()
        {
            this.bootstrapper = new AppBootstrapper();
            InitializeComponent();
        }
    }
}
