namespace Raven.ManagementStudio.UI.Silverlight.ViewModels
{
    using System.ComponentModel.Composition;
    using Caliburn.Micro;
    using Interfaces;
    using Messages;

    [Export(typeof(INavigationBar))]
    public class NavigationBarViewModel : Screen, INavigationBar, IHandle<ActiveScreenChangedMessage>
    {
        [ImportingConstructor]
        public NavigationBarViewModel(IEventAggregator eventAggregator)
        {
            eventAggregator.Subscribe(this);
        }

        public IEventAggregator Event { get; set; }

        public IRavenScreen ActiveScreen { get; set; }

        public void Handle(ActiveScreenChangedMessage message)
        {
            this.ActiveScreen = message.ActiveScreen;
        }
    }
}
