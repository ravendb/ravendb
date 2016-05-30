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
        private ExpiredDocumentsCleaner _expiredDocumentsCleaner;

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
                VersioningStorage = null;
                VersioningStorage = VersioningStorage.LoadConfigurations(_database);

                if (_log.IsDebugEnabled)
                    _log.Debug($"Versioning configuration was {(notification.Type == DocumentChangeTypes.Delete ? "disalbed" : "enabled")}");
            }
            else if(key.Equals(Constants.Expiration.RavenExpirationConfiguration, StringComparison.OrdinalIgnoreCase))
            {
                _expiredDocumentsCleaner = null;
                _expiredDocumentsCleaner = ExpiredDocumentsCleaner.LoadConfigurations(_database);

                if (_log.IsDebugEnabled)
                    _log.Debug($"Expiration configuration was {(_expiredDocumentsCleaner != null ? "enabled" : "disalbed")}");
            }
        }

        public void Dispose()
        {
            _database.Notifications.OnSystemDocumentChange -= HandleSystemDocumentChange;

            _expiredDocumentsCleaner?.Dispose();
        }

        public void DeleteDocument(DocumentsOperationContext context, string originalCollectionName, string key, bool isSystemDocument)
        {
            VersioningStorage?.Delete(context, originalCollectionName, key, isSystemDocument);
        }

        public void PutDocument(DocumentsOperationContext context, string originalCollectionName, string key, long newEtagBigEndian, 
            BlittableJsonReaderObject document, bool isSystemDocument)
        {
            if (isSystemDocument)
                return;

            VersioningStorage?.PutVersion(context, originalCollectionName, key, newEtagBigEndian, document);
            _expiredDocumentsCleaner?.Put(context, originalCollectionName, key, newEtagBigEndian, document);
        }
    }
}