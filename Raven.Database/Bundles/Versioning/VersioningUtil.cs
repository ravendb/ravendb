using System;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Bundles.Versioning.Data;
using Raven.Client.Connection;
using Raven.Database;
using Raven.Database.Config.Retriever;
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
            ConfigurationDocument<VersioningConfiguration> config = null;
            var entityName = metadata.Value<string>("Raven-Entity-Name");
            if (entityName != null)
                config = database.ConfigurationRetriever.GetConfigurationDocument<VersioningConfiguration>("Raven/Versioning/" + entityName);

            if (config == null)
                config = database.ConfigurationRetriever.GetConfigurationDocument<VersioningConfiguration>("Raven/Versioning/DefaultConfiguration");

            return config == null ? null : config.MergedDocument;
        }

        public static bool IsVersioningDisabledForImport(this DocumentDatabase database, RavenJObject metadata)
        {
            var ignoreVersioning = metadata.Value<string>(Constants.RavenIgnoreVersioning);
            return ignoreVersioning != null && ignoreVersioning.Equals("True", StringComparison.OrdinalIgnoreCase);
        }

        public static bool IsVersioningActive(this DocumentDatabase database, RavenJObject metadata)
        {
            var versioningConfiguration = database.GetDocumentVersioningConfiguration(metadata);
            return versioningConfiguration != null && versioningConfiguration.Exclude == false;
        }

        public static bool ChangesToRevisionsAllowed(this DocumentDatabase database)
        {
            var changesToRevisionsAllowed = database.Configuration.Settings[Constants.Versioning.ChangesToRevisionsAllowed];
            if (changesToRevisionsAllowed == null)
                return false;
            bool result;
            if (bool.TryParse(changesToRevisionsAllowed, out result) == false)
                return false;
            return result;
        }
    }
}
