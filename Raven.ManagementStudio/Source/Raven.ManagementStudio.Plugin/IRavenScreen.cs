namespace Raven.ManagementStudio.Plugin
{
    using Caliburn.Micro;

    public interface IRavenScreen : IScreen
    {
        void ChangeView(object view);
    }
}