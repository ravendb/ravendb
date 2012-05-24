using System.IO;
using Raven.Database.Plugins;
using Raven.Json.Linq;

namespace Raven.Bundles.Quotas.Size.Triggers
{
	public class DatabaseSizeQoutaForAttachmentsPutTrigger : AbstractAttachmentPutTrigger
	{
		public override VetoResult AllowPut(string key, Stream data, RavenJObject metadata)
		{
			return SizeQuotaConfiguration.GetConfiguration(Database).AllowPut();
		}
	}
}