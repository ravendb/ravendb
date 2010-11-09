namespace Raven.ManagementStudio.UI.Silverlight.Messages
{
    using ViewModels.Interfaces;

    public class ActiveScreenChangedMessage
    {
        public ActiveScreenChangedMessage(IRavenScreen screen)
        {
            this.ActiveScreen = screen;
        }

        public IRavenScreen ActiveScreen { get; private set; }
    }
}