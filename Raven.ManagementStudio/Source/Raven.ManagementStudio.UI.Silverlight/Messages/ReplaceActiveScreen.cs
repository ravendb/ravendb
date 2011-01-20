namespace Raven.ManagementStudio.UI.Silverlight.Messages
{
    using Plugin;

    public class ReplaceActiveScreen
    {
        public ReplaceActiveScreen(IRavenScreen screen)
        {
            this.NewScreen = screen;
        }

        public IRavenScreen NewScreen { get; private set; }
    }
}