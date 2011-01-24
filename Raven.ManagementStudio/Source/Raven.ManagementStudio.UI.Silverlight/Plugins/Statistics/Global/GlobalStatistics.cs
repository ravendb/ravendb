using System.ComponentModel.Composition;
using Raven.ManagementStudio.Plugin;

namespace Raven.ManagementStudio.UI.Silverlight.Plugins.Statistics.Global
{
    [Export(typeof(IPlugin))]
    public class GlobalStatistics : PluginBase
    {
        public override string Name
        {
            get { return "GLOBAL"; }
        }

        public override SectionType Section
        {
            get { return SectionType.Statistics; }
        }

        public override IRavenScreen RelatedScreen
        {
            get { return new GlobalStatisticsScreenViewModel(Database); }
        }

        public override object MenuView
        {
            get { return new GlobalStatisticsMenuIcon(); }
        }
    }
}
