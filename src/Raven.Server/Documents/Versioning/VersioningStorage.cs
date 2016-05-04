using System;
using System.Net;
using Raven.Abstractions.Data;
using Raven.Abstractions.Logging;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Voron.Data.BTrees;
using Voron.Data.Tables;

namespace Raven.Server.Documents.Versioning
{
    public unsafe class VersioningStorage : IDisposable
    {
        private readonly ILog Log = LogManager.GetLogger(typeof(VersioningStorage));

        private readonly DocumentDatabase _database;
        private Document _versioningConfiguration;

        public VersioningStorage(DocumentDatabase database)
        {
            _database = database;
            _database.Notifications.OnSystemDocumentChange += HandleSystemDocumentChange;
            LoadConfigurations();
        }

        private void LoadConfigurations()
        {
            DocumentsOperationContext context;
            using (_database.DocumentsStorage.ContextPool.AllocateOperationContext(out context))
            {
                context.OpenReadTransaction();

                _versioningConfiguration = _database.DocumentsStorage.Get(context, Constants.Versioning.RavenVersioningConfiguration);
            }
        }

        private void HandleSystemDocumentChange(DocumentChangeNotification notification)
        {
            if (notification.Key.Equals(Constants.Versioning.RavenVersioningConfiguration, StringComparison.OrdinalIgnoreCase) == false)
                return;

            _versioningConfiguration = null;
            LoadConfigurations();

            if (Log.IsDebugEnabled)
                Log.Debug(() => $"Versioning configuration was changed");
        }

        public void Dispose()
        {
            _database.Notifications.OnSystemDocumentChange -= HandleSystemDocumentChange;
        }

        public bool IsChangesToRevisionsAllowed(string collectionName)
        {
            return IsConfigurationPropertyTrue("ChangesToRevisionsAllowed", collectionName);
        }

        public bool IsVersioningActive(string collectionName)
        {
            return IsConfigurationPropertyTrue("Active", collectionName);
        }

        public bool IsConfigurationPropertyTrue(string propertyName, string collectionName)
        {
            if (_versioningConfiguration == null)
                return false;

            BlittableJsonReaderObject configuration;
            if (_versioningConfiguration.Data.TryGet(collectionName, out configuration))
            {
                bool active;
                if (configuration.TryGet(propertyName, out active))
                    return active;

                return false;
            }

            if (_versioningConfiguration.Data.TryGet("DefaultConfiguration", out configuration))
            {
                bool active;
                if (configuration.TryGet(propertyName, out active))
                    return active;

                return false;
            }

            return false;
        }

        /*public void AssertAllowPut(string collectionName, string key, BlittableJsonReaderObject document)
        {
            if (IsVersioningActive(collectionName) == false)
                return;

            BlittableJsonReaderObject metadata;
            if (document.TryGet(Constants.Metadata, out metadata))
            {
                bool ignoreVersioning;
                if (metadata.TryGet(Constants.Versioning.RavenIgnoreVersioning, out ignoreVersioning))
                {
                    metadata.Modifications.Remove(Constants.Versioning.RavenIgnoreVersioning);
                    if (ignoreVersioning)
                        return;
                }
            }
        }*/

        public void PutVersion(DocumentsOperationContext context, string collectionName, string key, BlittableJsonReaderObject document, TableValueReader oldValue, bool isSystemDocument)
        {
            if (isSystemDocument)
                return;

            bool enableVersioning;
            BlittableJsonReaderObject metadata;
            if (document.TryGet(Constants.Metadata, out metadata))
            {
                if (metadata.TryGet(Constants.Versioning.RavenEnableVersioning, out enableVersioning))
                {
                    metadata.Modifications.Remove(Constants.Versioning.RavenEnableVersioning);
                }

                bool disableVersioning;
                if (metadata.TryGet(Constants.Versioning.RavenDisableVersioning, out disableVersioning))
                {
                    metadata.Modifications.Remove(Constants.Versioning.RavenDisableVersioning);
                    if (disableVersioning)
                        return;
                }
            }

            if (IsVersioningActive(collectionName) == false)
                return;



            var revisionCounter = IncrementRevisionNumber(context, collectionName, key);
            key += "/revisions/" + revisionCounter;

            byte* lowerKey;
            int lowerSize;
            byte* keyPtr;
            int keySize;
            GetLowerKeySliceAndStorageKey(context, key, out lowerKey, out lowerSize, out keyPtr, out keySize);

            var newEtag = ++_lastEtag;
            var newEtagBigEndian = IPAddress.HostToNetworkOrder(newEtag);

            /*  var versioned = new DynamicJsonValue(document);
              versioned.Remove(Constants.RavenCreateVersion);
              versioned.Remove(Constants.RavenIgnoreVersioning);
              var versionedDocument = context.ReadObject(versioned, key, BlittableJsonDocumentBuilder.UsageMode.ToDisk);*/

            int size;
            var basePointer = oldValue.Read(3, out size);

            var tbv = new TableValueBuilder
            {
                {lowerKey, lowerSize},
                {(byte*)&newEtagBigEndian, sizeof(long)},
                {keyPtr, keySize},
                {basePointer, size}
            };

            var table = new Table(_docsSchema, "_revisions/" + collectionName, context.Transaction.InnerTransaction);
            var insert = table.Insert(tbv);
        }
    }
}