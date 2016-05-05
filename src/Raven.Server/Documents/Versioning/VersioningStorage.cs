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
        private readonly TableSchema _docsSchema = new TableSchema();

        private Document _versioningConfiguration;

        // this is only modified by write transactions under lock
        // no need to use thread safe ops
        private long _lastEtag;

        public VersioningStorage(DocumentDatabase database)
        {
            _database = database;

            // The documents schema is as follows
            // 4 fields (lowered key, etag, lazy string key, document)
            // format of lazy string key is detailed in GetLowerKeySliceAndStorageKey
            _docsSchema.DefineIndex("KeyAndEtag", new TableSchema.SchemaIndexDef
            {
                StartIndex = 0,
                Count = 2,
                IsGlobal = true,
            });
            _docsSchema.DefineIndex("Key", new TableSchema.SchemaIndexDef
            {
                StartIndex = 0,
                Count = 1,
                IsGlobal = true,
            });
            _docsSchema.DefineFixedSizeIndex("AllDocsEtags", new TableSchema.FixedSizeSchemaIndexDef
            {
                StartIndex = 1,
                IsGlobal = true
            });

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

        private bool IsVersioningActive(string collectionName, bool explictEnableVersioning, out BlittableJsonReaderObject configuration)
        {
            configuration = null;
            if (_versioningConfiguration == null)
                return false;

            if (_versioningConfiguration.Data.TryGet(collectionName, out configuration))
            {
                return IsVersioningActiveForCollection(configuration, explictEnableVersioning);
            }

            if (_versioningConfiguration.Data.TryGet("DefaultConfiguration", out configuration))
            {
                return IsVersioningActiveForCollection(configuration, explictEnableVersioning);
            }

            return false;
        }

        private static bool IsVersioningActiveForCollection(BlittableJsonReaderObject configuration, bool explictEnableVersioning)
        {
            bool active;
            if (configuration.TryGet("Active", out active) && active)
                return true;

            bool activeIfExplicit;
            if (configuration.TryGet("ActiveIfExplicit", out activeIfExplicit) && activeIfExplicit && explictEnableVersioning)
            {
                return true;
            }

            return false;
        }

        public void PutVersion(DocumentsOperationContext context, string collectionName, string key, 
            BlittableJsonReaderObject document, BlittableJsonReaderObject oldDocument, bool isSystemDocument)
        {
            if (isSystemDocument)
                return;

            bool enableVersioning = false;
            BlittableJsonReaderObject metadata;
            if (document != null && document.TryGet(Constants.Metadata, out metadata))
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

            BlittableJsonReaderObject configuration;
            if (IsVersioningActive(collectionName, enableVersioning, out configuration) == false)
                return;

            var table = new Table(_docsSchema, "_revisions/" + collectionName, context.Transaction.InnerTransaction);

            int? maxRevisions;
            configuration.TryGet("MaxRevisions", out maxRevisions);
            var revisionsCount = IncrementCountOfRevisions(context, key, 1);
            DeleteOldRevisions(context, table, key, maxRevisions, revisionsCount);

            byte* lowerKey;
            int lowerSize;
            byte* keyPtr;
            int keySize;
            DocumentsStorage.GetLowerKeySliceAndStorageKey(context, key, out lowerKey, out lowerSize, out keyPtr, out keySize);

            var newEtag = ++_lastEtag;
            var newEtagBigEndian = IPAddress.HostToNetworkOrder(newEtag);

            var tbv = new TableValueBuilder
            {
                {lowerKey, lowerSize},
                {(byte*)&newEtagBigEndian, sizeof(long)},
                {keyPtr, keySize},
                {oldDocument.BasePointer, oldDocument.Size}
            };

            var insert = table.Insert(tbv);
        }

        private void DeleteOldRevisions(DocumentsOperationContext context, Table table, string key, int? maxRevisions, long revisionsCount)
        {
            if (maxRevisions.HasValue == false || maxRevisions.Value == int.MaxValue)
                return;

            var numberOfRevisionsToDelete = revisionsCount - maxRevisions.Value;
            if (numberOfRevisionsToDelete <= 0)
                return;

            var deletedRevisionsCount = table.DeleteForwardFrom(_docsSchema.Indexes["KeyAndEtag"], key, numberOfRevisionsToDelete);
            IncrementCountOfRevisions(context, key, -deletedRevisionsCount);
        }

        private long IncrementCountOfRevisions(DocumentsOperationContext context, string key, long delta)
        {
            var numbers = context.Transaction.InnerTransaction.ReadTree("VersioningRevisionsCount");
            return numbers.Increment(key, delta);
        }

        private void DeleteCountOfRevisions(DocumentsOperationContext context, string key)
        {
            var numbers = context.Transaction.InnerTransaction.ReadTree("VersioningRevisionsCount");
            numbers.Delete(key);
        }

        public void Delete(DocumentsOperationContext context, string collectionName, string key, Document document, bool isSystemDocument)
        {
            if (isSystemDocument)
                return;

            BlittableJsonReaderObject configuration;
            if (IsVersioningActive(collectionName, false, out configuration) == false)
                return;

            bool purgeOnDelete;
            configuration.TryGet("PurgeOnDelete", out purgeOnDelete);

            if (purgeOnDelete)
            {
                DeleteCountOfRevisions(context, key);
                var table = new Table(_docsSchema, "_revisions/" + collectionName, context.Transaction.InnerTransaction);
                table.DeleteByKey(key);
            }
            else
            {
                PutVersion(context, collectionName, key, null, document.Data, isSystemDocument);
            }
        }
    }
}