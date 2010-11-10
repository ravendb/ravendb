namespace Raven.ManagementStudio.UI.Silverlight.Messages
{
    using Plugin;

    public class ActiveScreenChanged
    {
        public ActiveScreenChanged(IRavenScreen screen)
        {
            this.ActiveScreen = screen;
        }

        public IRavenScreen ActiveScreen { get; private set; }
    }
}