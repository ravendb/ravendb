using System.ComponentModel.Composition;
using Raven.Bundles.Quotas.Size;
using Raven.Database.Plugins;

namespace Raven.Bundles.Quotas.Documents.Triggers
{

    [InheritedExport(typeof(AbstractDeleteTrigger))]
    [ExportMetadata("Bundle", "Quotas")]
    public class DatabaseCountDocumentDeleteTrigger : AbstractDeleteTrigger
    {
        public override void AfterDelete(string key)
        {
            using (Database.DisableAllTriggersForCurrentThread())
            {
                DocQuotaConfiguration.GetConfiguration(Database).AfterDelete();
            }
        }
    }
}
