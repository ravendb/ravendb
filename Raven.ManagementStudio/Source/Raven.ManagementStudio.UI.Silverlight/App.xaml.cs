using System.Windows;

namespace Raven.ManagementStudio.UI.Silverlight
{
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
