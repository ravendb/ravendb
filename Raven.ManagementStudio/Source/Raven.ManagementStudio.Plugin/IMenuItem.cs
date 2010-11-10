namespace Raven.ManagementStudio.Plugin
{
    public interface IMenuItem
    {
        ISection Section { get; }

        IRavenScreen RelatedScreen { get; }

        object MenuView { get; }
    }
}
