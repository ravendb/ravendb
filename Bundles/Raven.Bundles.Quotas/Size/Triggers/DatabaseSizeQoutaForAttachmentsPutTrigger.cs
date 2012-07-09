using System.ComponentModel.Composition;
using System.IO;
using Raven.Database.Plugins;
using Raven.Json.Linq;

namespace Raven.Bundles.Quotas.Size.Triggers
{
	[InheritedExport(typeof(AbstractAttachmentPutTrigger))]
	[ExportMetadata("Bundle", "Quotas")]
	public class DatabaseSizeQoutaForAttachmentsPutTrigger : AbstractAttachmentPutTrigger
	{
		public override VetoResult AllowPut(string key, Stream data, RavenJObject metadata)
		{
			return SizeQuotaConfiguration.GetConfiguration(Database).AllowPut();
		}
	}
}