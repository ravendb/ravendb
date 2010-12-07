using System.Diagnostics;
using System.Windows;
using System.Windows.Controls;
using Raven.ManagementStudio.UI.Silverlight.Controls;

namespace Raven.ManagementStudio.UI.Silverlight
{
    public partial class App : Application
    {
        // ReSharper disable UnaccessedField.Local
        private readonly AppBootstrapper _bootstrapper;
        // ReSharper restore UnaccessedField.Local

        public App()
        {
            UnhandledException += OnUnhandledException;

            _bootstrapper = new AppBootstrapper();

            InitializeComponent();
        }

        private static void OnUnhandledException(object sender, ApplicationUnhandledExceptionEventArgs e)
        {
            if (!Debugger.IsAttached)
            {
                e.Handled = true;
                var errorWin = new ErrorWindow(e.ExceptionObject);
                errorWin.Show();
            }
        }
    }
}
