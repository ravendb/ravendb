namespace Raven.ManagementStudio.Plugin
{
    public interface IMenuItem
    {
        SectionType Section { get; }

        IRavenScreen RelatedScreen { get; }

        object MenuView { get; }

        int Ordinal { get; }

        bool IsActive { get; set; }
    }
}
