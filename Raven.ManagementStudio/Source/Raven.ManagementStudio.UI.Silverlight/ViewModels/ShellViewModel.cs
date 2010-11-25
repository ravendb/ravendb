namespace Raven.ManagementStudio.UI.Silverlight.ViewModels
{
    using System.ComponentModel.Composition;
    using System.Windows;
    using Caliburn.Micro;
    using Interfaces;
    using Messages;
    using Models;

    [Export(typeof(IShell))]
    public class ShellViewModel : Conductor<DatabaseViewModel>.Collection.OneActive, IShell, IHandle<OpenNewScreen>
    {
        private readonly INavigationBar navigationBar;

        [ImportingConstructor]
        public ShellViewModel(INavigationBar navigationBar, IEventAggregator eventAggregator)
        {
            this.navigationBar = navigationBar;
            ActivateItem(new DatabaseViewModel(new Database("http://localhost:8080", "Local")));
            ActivateItem(new DatabaseViewModel(new Database("http://localhost:8080", "Raven")));
            eventAggregator.Subscribe(this);
        }

        public INavigationBar NavigationBar
        {
            get { return this.navigationBar; }
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

        public void DragWindow()
        {
            Application.Current.MainWindow.DragMove();
        }

        public void ResizeWindow(string direction)
        {
            WindowResizeEdge edge;
            WindowResizeEdge.TryParse(direction, out edge);
            Application.Current.MainWindow.DragResize(edge);
        }

        public void Handle(OpenNewScreen message)
        {
            this.ActiveItem.ActivateItem(message.NewScreen);
        }
    }
}
