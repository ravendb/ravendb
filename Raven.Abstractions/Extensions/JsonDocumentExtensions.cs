namespace Raven.Abstractions.Extensions
{
	using System.Linq;

	using Raven.Abstractions.Data;
	using Raven.Json.Linq;

	public static class JsonDocumentExtensions
	{
		public static bool IsConflictDocument(this JsonDocument document)
		{
			var conflict = document.Metadata.Value<RavenJValue>(Constants.RavenReplicationConflict);
			if (conflict == null || conflict.Value<bool>() == false)
			{
				return false;
			}

			var keyParts = document.Key.Split('/');
			if (keyParts.Contains("conflicts") == false)
			{
				return false;
			}

			var conflicts = document.DataAsJson.Value<RavenJArray>("Conflicts");
			if (conflicts != null)
			{
				return false;
			}

			return true;
		}
	}
}