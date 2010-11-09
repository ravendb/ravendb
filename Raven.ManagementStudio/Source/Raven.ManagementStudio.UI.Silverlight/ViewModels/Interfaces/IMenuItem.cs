namespace Raven.ManagementStudio.UI.Silverlight.ViewModels.Interfaces
{
    public interface IMenuItem
    {
        ISection Section { get; set; }
        IRavenScreen RelatedScreen { get; set; }      
    }
}
