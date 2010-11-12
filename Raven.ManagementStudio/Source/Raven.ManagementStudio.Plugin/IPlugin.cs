namespace Raven.ManagementStudio.Plugin
{
    public interface IPlugin : IMenuItem
    {
        string Name { get; }

        IDatabase Database { get; set; }

        void GoToScreen();
    }
}