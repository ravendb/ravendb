namespace Raven.ManagementStudio.Plugin
{
    public interface IPlugin : IMenuItem
    {
        string Name { get; }

        void GoToScreen();
    }
}