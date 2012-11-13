namespace Raven.Abstractions.Extensions
{
	using System.Linq;

	using Raven.Abstractions.Data;
	using Raven.Json.Linq;

	public static class AttachmentExtensions
	{
		public static bool IsConflictAttachment(this Attachment attachment)
		{
			var conflict = attachment.Metadata.Value<RavenJValue>(Constants.RavenReplicationConflict);
			if (conflict == null || conflict.Value<bool>() == false)
			{
				return false;
			}

			var keyParts = attachment.Key.Split('/');
			if (keyParts.Contains("conflicts") == false)
			{
				return false;
			}

			var conflicts = attachment.Data().ToJObject().Value<RavenJArray>("Conflicts");
			if (conflicts != null)
			{
				return false;
			}

			return true;
		} 
	}
}