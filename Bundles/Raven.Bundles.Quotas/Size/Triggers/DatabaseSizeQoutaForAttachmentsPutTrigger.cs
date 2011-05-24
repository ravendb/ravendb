using System;
using Raven.Abstractions.Data;
using Raven.Database.Commercial;
using Raven.Database.Plugins;
using Raven.Json.Linq;

namespace Raven.Bundles.Quotas.Triggers
{
	public class DatabaseSizeQoutaForAttachmentsPutTrigger : AbstractAttachmentPutTrigger
	{
		public override VetoResult AllowPut(string key, byte[] data, RavenJObject metadata)
		{
			return SizeQuotaConfiguration.GetConfiguration(Database).AllowPut();
		}
	}
}