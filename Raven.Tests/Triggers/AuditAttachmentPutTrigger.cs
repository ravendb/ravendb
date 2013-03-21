using System;
using System.IO;
using Raven.Database.Plugins;
using Raven.Json.Linq;

namespace Raven.Tests.Triggers
{
	public class AuditAttachmentPutTrigger : AbstractAttachmentPutTrigger
	{
		public override void OnPut(string key, Stream data, RavenJObject metadata)
		{
			metadata["created_at"] = new RavenJValue(new DateTime(2000, 1, 1, 0, 0, 0, DateTimeKind.Utc));
		}
	}
}