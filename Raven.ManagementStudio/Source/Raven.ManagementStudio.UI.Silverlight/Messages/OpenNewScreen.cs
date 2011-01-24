namespace Raven.ManagementStudio.UI.Silverlight.Messages
{
    using Plugin;

    public class OpenNewScreen
    {
        public OpenNewScreen(IRavenScreen screen)
        {
            this.NewScreen = screen;
        }

        public IRavenScreen NewScreen { get; private set; }
    }
}