using System;
using Raven.Abstractions.Data;
using Raven.Abstractions.Logging;
using Raven.Server.Documents.Versioning;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents
{
    public class BundleLoader : IDisposable
    {
        private readonly ILog _log = LogManager.GetLogger(typeof(BundleLoader));

        private readonly DocumentDatabase _database;
        private VersioningStorage _versioningStorage;

        public BundleLoader(DocumentDatabase database)
        {
            _database = database;
            _database.Notifications.OnSystemDocumentChange += HandleSystemDocumentChange;
        }

        public void HandleSystemDocumentChange(DocumentChangeNotification notification)
        {
            if (notification.Key.Equals(Constants.Versioning.RavenVersioningConfiguration, StringComparison.OrdinalIgnoreCase) != false)
            {
                _versioningStorage = null;
                _versioningStorage = VersioningStorage.LoadConfigurations(_database);

                if (_log.IsDebugEnabled)
                    _log.Debug($"Versioning configuration was {(notification.Type == DocumentChangeTypes.Delete ? "disalbed" : "enabled")}");
            }
        }

        public void Dispose()
        {
            _database.Notifications.OnSystemDocumentChange -= HandleSystemDocumentChange;
        }

        public void DeleteDocument(DocumentsOperationContext context, string originalCollectionName, string key, bool isSystemDocument)
        {
            _versioningStorage?.Delete(context, originalCollectionName, key, isSystemDocument);
        }

        public void PutDocument(DocumentsOperationContext context, string originalCollectionName, string key, long newEtagBigEndian, BlittableJsonReaderObject document, bool isSystemDocument)
        {
            _versioningStorage?.PutVersion(context, originalCollectionName, key, newEtagBigEndian, document, isSystemDocument);
        }
    }
}