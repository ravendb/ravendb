using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Bundles.Versioning.Data;
using Raven.Database;
using Raven.Json.Linq;

namespace Raven.Bundles.Versioning
{
	internal static class VersioningUtil
	{
		public const string RavenDocumentRevision = "Raven-Document-Revision";
		public const string RavenDocumentParentRevision = "Raven-Document-Parent-Revision";
		public const string RavenDocumentRevisionStatus = "Raven-Document-Revision-Status";

		public static VersioningConfiguration GetDocumentVersioningConfiguration(this DocumentDatabase database, RavenJObject metadata)
		{
			JsonDocument doc = null;

			var entityName = metadata.Value<string>("Raven-Entity-Name");
			if (entityName != null)
				doc = database.Get("Raven/Versioning/" + entityName, null);

			if (doc == null)
				doc = database.Get("Raven/Versioning/DefaultConfiguration", null);

			if (doc == null)
				return null;

			return doc.DataAsJson.JsonDeserialization<VersioningConfiguration>();
		}

		public static bool IsVersioningActive(this DocumentDatabase database, RavenJObject metadata)
		{
			var versioningConfiguration = database.GetDocumentVersioningConfiguration(metadata);
			return versioningConfiguration != null && versioningConfiguration.Exclude == false;
		}
	}
}