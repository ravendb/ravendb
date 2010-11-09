namespace Raven.ManagementStudio.UI.Silverlight.ViewModels
{
    using System.ComponentModel.Composition;
    using System.Windows;
    using Caliburn.Micro;
    using Interfaces;

    [Export(typeof(IShell))]
    public class ShellViewModel : Screen, IShell
    {
        private readonly INavigationBar navigationBar;
        private readonly RavenScreensViewModel ravenScreens;

        [ImportingConstructor]
        public ShellViewModel(INavigationBar navigationBar, RavenScreensViewModel ravenScreens)
        {
            this.navigationBar = navigationBar;
            this.ravenScreens = ravenScreens;
        }

        public INavigationBar NavigationBar
        {
            get { return navigationBar; }
        }

        public RavenScreensViewModel RavenScreens
        {
            get { return ravenScreens; }
        }

        public void CloseWindow()
        {
            Application.Current.MainWindow.Close();
        }

        public void ToogleWindow()
        {
            Application.Current.MainWindow.WindowState = Application.Current.MainWindow.WindowState == WindowState.Normal ? WindowState.Maximized : WindowState.Normal;
        }

        public void MinimizeWindow()
        {
            Application.Current.MainWindow.WindowState = WindowState.Minimized;
        }
    }
}
