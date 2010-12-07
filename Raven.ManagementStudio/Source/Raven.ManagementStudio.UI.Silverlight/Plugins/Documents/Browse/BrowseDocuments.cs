using System.ComponentModel.Composition;
using Raven.ManagementStudio.Plugin;

namespace Raven.ManagementStudio.UI.Silverlight.Plugins.Documents.Browse
{
    [Export(typeof(IPlugin))]
    public class BrowseDocuments : PluginBase
    {
        public override string Name
        {
            get { return "BROWSE"; }
        }

        public override SectionType Section
        {
            get { return SectionType.Documents; }
        }

        public override IRavenScreen RelatedScreen
        {
            get { return new DocumentsScreenViewModel(Database); }
        }

        public override object MenuView
        {
            get { return new BrowseMenuIcon(); }
        }
    }
}
