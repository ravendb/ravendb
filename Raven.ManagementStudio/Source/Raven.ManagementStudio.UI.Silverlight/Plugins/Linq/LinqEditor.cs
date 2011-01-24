using System.ComponentModel.Composition;
using Raven.ManagementStudio.Plugin;

namespace Raven.ManagementStudio.UI.Silverlight.Plugins.Linq
{
    [Export(typeof(IPlugin))]
    public class LinqEditor : PluginBase
    {
        public override SectionType Section
        {
            get { return SectionType.Linq; }
        }

        public override IRavenScreen RelatedScreen
        {
            get { return new LinqEditorViewModel(Database); }
        }

        public override string Name
        {
            get { return "EDITOR"; }
        }
    }
}
