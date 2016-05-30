using System;
using Raven.Abstractions.Data;
using Raven.Abstractions.Logging;
using Raven.Server.Documents.Expiration;
using Raven.Server.Documents.Versioning;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents
{
    public class BundleLoader : IDisposable
    {
        private readonly ILog _log = LogManager.GetLogger(typeof(BundleLoader));

        private readonly DocumentDatabase _database;
        public VersioningStorage VersioningStorage;
        public ExpiredDocumentsCleaner ExpiredDocumentsCleaner;

        public BundleLoader(DocumentDatabase database)
        {
            _database = database;
            _database.Notifications.OnSystemDocumentChange += HandleSystemDocumentChange;
        }

        public void HandleSystemDocumentChange(DocumentChangeNotification notification)
        {
            var key = notification.Key;
            if (key.Equals(Constants.Versioning.RavenVersioningConfiguration, StringComparison.OrdinalIgnoreCase))
            {
                VersioningStorage = VersioningStorage.LoadConfigurations(_database);

                if (_log.IsDebugEnabled)
                    _log.Debug($"Versioning configuration was {(VersioningStorage  != null ? "disabled" : "enabled")}");
            }
            else if(key.Equals(Constants.Expiration.RavenExpirationConfiguration, StringComparison.OrdinalIgnoreCase))
            {
                ExpiredDocumentsCleaner?.Dispose();
                ExpiredDocumentsCleaner = ExpiredDocumentsCleaner.LoadConfigurations(_database);

                if (_log.IsDebugEnabled)
                    _log.Debug($"Expiration configuration was {(ExpiredDocumentsCleaner != null ? "enabled" : "disabled")}");
            }
        }

        public void Dispose()
        {
            _database.Notifications.OnSystemDocumentChange -= HandleSystemDocumentChange;

            ExpiredDocumentsCleaner?.Dispose();
        }
    }
}