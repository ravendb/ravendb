using System.ComponentModel.Composition;
using Raven.ManagementStudio.Plugin;

namespace Raven.ManagementStudio.UI.Silverlight.Plugins.Indexes.Browse
{
    [Export(typeof(IPlugin))]
    public class BrowseIndexes : PluginBase
    {
        public override string Name
        {
            get { return "BROWSE"; }
        }

        public override SectionType Section
        {
            get { return SectionType.Indexes; }
        }

        public override IRavenScreen RelatedScreen
        {
            get { return new BrowseIndexesScreenViewModel(Database); }
        }

        public override object MenuView
        {
            get { return new BrowseIndexesMenuIcon(); }
        }
    }
}