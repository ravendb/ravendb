using System.ComponentModel.Composition;
using Raven.ManagementStudio.Plugin;
using Raven.ManagementStudio.UI.Silverlight.Models;
using Raven.ManagementStudio.UI.Silverlight.Plugins.Common;
using Raven.Database;

namespace Raven.ManagementStudio.UI.Silverlight.Plugins.Documents.Browse
{
    [Export(typeof(IPlugin))]
    public class CreateNewDocument : PluginBase
    {
        public override SectionType Section
        {
            get { return SectionType.Documents; }
        }

        public override IRavenScreen RelatedScreen
        {
            get
            {
                return new DocumentViewModel(new Document(new JsonDocument
                {
                    DataAsJson = new Newtonsoft.Json.Linq.JObject(),
                    Metadata = new Newtonsoft.Json.Linq.JObject()
                }), Database, null);
            }
        }

        public override object MenuView
        {
            get { return null; }
        }

        public override string Name
        {
            get { return "CREATE NEW"; }
        }

        public override int Ordinal
        {
            get { return 1; }
        }
    }
}
