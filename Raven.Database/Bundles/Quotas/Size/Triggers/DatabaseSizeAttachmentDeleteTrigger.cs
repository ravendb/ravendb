using System.ComponentModel.Composition;
using Raven.Database.Plugins;

namespace Raven.Bundles.Quotas.Size.Triggers
{
	[InheritedExport(typeof(AbstractAttachmentDeleteTrigger))]
	[ExportMetadata("Bundle", "Quotas")]
	public class DatabaseSizeAttachmentDeleteTrigger : AbstractAttachmentDeleteTrigger
	{
		public override void AfterDelete(string key)
		{
			SizeQuotaConfiguration.GetConfiguration(Database).AfterDelete();
		}
	}
}