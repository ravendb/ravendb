using System.IO;
using Raven.Database.Plugins;
using Raven.Json.Linq;

namespace Raven.Tests.Triggers
{
	public class RefuseBigAttachmentPutTrigger : AbstractAttachmentPutTrigger
	{
		public override VetoResult AllowPut(string key, Stream data, RavenJObject metadata)
		{
			if (data.Length > 4)
				return VetoResult.Deny("Attachment is too big");

			return VetoResult.Allowed;
		}
	}
}