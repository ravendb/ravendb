using System;

namespace Raven.ManagementStudio.UI.Silverlight.ViewModels
{
    using System.ComponentModel.Composition;
    using System.Windows;
    using Caliburn.Micro;
    using Interfaces;
    using Messages;
    using Models;
    using System.Windows.Threading;

    [Export(typeof(IShell))]
    public class ShellViewModel : Conductor<DatabaseViewModel>.Collection.OneActive, IShell, IHandle<OpenNewScreen>
    {
        [ImportingConstructor]
        public ShellViewModel(IEventAggregator eventAggregator)
        {
            ActivateItem(new DatabaseViewModel(new Database("http://localhost:8080", "Local")));
            eventAggregator.Subscribe(this);
        }
    
        public Window Window
        {
            get { return Application.Current.MainWindow; }
        }

        public void CloseWindow()
        {
            Window.Close();
        }

        public void ToogleWindow()
        {
            Window.WindowState = Window.WindowState == WindowState.Normal ? WindowState.Maximized : WindowState.Normal;
        }

        public void MinimizeWindow()
        {
            Window.WindowState = WindowState.Minimized;
        }

        public void DragWindow()
        {
            Window.DragMove();
        }

        public void ResizeWindow(string direction)
        {
            WindowResizeEdge edge;
            Enum.TryParse(direction, out edge);
            Window.DragResize(edge);
        }

        public void Handle(OpenNewScreen message)
        {
            ActiveItem.ActivateItem(message.NewScreen);
        }
    }
}
