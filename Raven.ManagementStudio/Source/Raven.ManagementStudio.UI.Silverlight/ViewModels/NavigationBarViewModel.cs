namespace Raven.ManagementStudio.UI.Silverlight.ViewModels
{
    using System.ComponentModel.Composition;
    using Caliburn.Micro;
    using Interfaces;
    using Messages;
    using Plugin;

    [Export(typeof(INavigationBar))]
    public class NavigationBarViewModel : Screen, INavigationBar, IHandle<ActiveScreenChanged>
    {
        private IRavenScreen activeScreen;

        [ImportingConstructor]
        public NavigationBarViewModel(IEventAggregator eventAggregator)
        {
            eventAggregator.Subscribe(this);
        }

        public IRavenScreen ActiveScreen
        {
            get
            {
                return this.activeScreen;
            }

            set
            {
                this.activeScreen = value;
                NotifyOfPropertyChange(() => this.ActiveScreen);
            }
        }

        public void Handle(ActiveScreenChanged message)
        {
            this.ActiveScreen = message.ActiveScreen;
        }
    }
}
