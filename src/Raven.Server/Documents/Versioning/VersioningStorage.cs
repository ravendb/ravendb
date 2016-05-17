using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net;
using Raven.Abstractions.Data;
using Raven.Abstractions.Logging;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Voron.Data.Tables;

namespace Raven.Server.Documents.Versioning
{
    public unsafe class VersioningStorage : IDisposable
    {
        private readonly ILog Log = LogManager.GetLogger(typeof(VersioningStorage));

        private readonly DocumentDatabase _database;
        private readonly TableSchema _docsSchema = new TableSchema();

        private readonly VersioningConfiguration _versioningConfiguration;

        private const string VersioningRevisionsCount = "_VersioningRevisionsCount";

        private readonly VersioningConfigurationCollection _emptyConfiguration = new VersioningConfigurationCollection();

        public VersioningStorage(DocumentDatabase database, VersioningConfiguration versioningConfiguration)
        {
            _database = database;
            _versioningConfiguration = versioningConfiguration;

            // The documents schema is as follows
            // 4 fields (lowered key, etag, lazy string key, document)
            // format of lazy string key is detailed in GetLowerKeySliceAndStorageKey
            _docsSchema.DefineIndex("KeyAndEtag", new TableSchema.SchemaIndexDef
            {
                StartIndex = 0,
                Count = 2,
            });
        }

        public static VersioningStorage LoadConfigurations(DocumentDatabase database)
        {
            DocumentsOperationContext context;
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out context))
            {
                context.OpenReadTransaction();

                var configuration = database.DocumentsStorage.Get(context, Constants.Versioning.RavenVersioningConfiguration);
                if (configuration == null)
                    return null;

                var versioningConfiguration = JsonDeserialization.VersioningConfiguration(configuration.Data);
                return new VersioningStorage(database, versioningConfiguration);
            }
        }

        public void Dispose()
        {
        }

        private VersioningConfigurationCollection GetVersioningConfiguration(string collectionName)
        {
            VersioningConfigurationCollection configuration;
            if (_versioningConfiguration.Collections != null && _versioningConfiguration.Collections.TryGetValue(collectionName, out configuration))
            {
                return configuration;
            }

            if (_versioningConfiguration.Default != null)
            {
                return _versioningConfiguration.Default;
            }

            return _emptyConfiguration;
        }

        public void PutVersion(DocumentsOperationContext context, string collectionName, string key, long newEtagBigEndian,
            BlittableJsonReaderObject document, bool isSystemDocument)
        {
            if (isSystemDocument)
                return;

            var enableVersioning = false;
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

            var configuration = GetVersioningConfiguration(collectionName);
            if (enableVersioning == false && configuration.Active == false)
                return;

            var table = new Table(_docsSchema, "_revisions/" + collectionName, context.Transaction.InnerTransaction);
            var revisionsCount = IncrementCountOfRevisions(context, key, 1);
            DeleteOldRevisions(context, table, key, configuration.MaxRevisions, revisionsCount);

            byte* lowerKey;
            int lowerSize;
            byte* keyPtr;
            int keySize;
            DocumentsStorage.GetLowerKeySliceAndStorageKey(context, key, out lowerKey, out lowerSize, out keyPtr, out keySize);

            var tbv = new TableValueBuilder
            {
                {lowerKey, lowerSize},
                {(byte*)&newEtagBigEndian, sizeof(long)},
                {keyPtr, keySize},
                {document.BasePointer, document.Size}
            };

            table.Insert(tbv);
        }

        private void DeleteOldRevisions(DocumentsOperationContext context, Table table, string key, int? maxRevisions, long revisionsCount)
        {
            if (maxRevisions.HasValue == false || maxRevisions.Value == int.MaxValue)
                return;

            var numberOfRevisionsToDelete = revisionsCount - maxRevisions.Value;
            if (numberOfRevisionsToDelete <= 0)
                return;

            var deletedRevisionsCount = table.DeleteForwardFrom(_docsSchema.Indexes["KeyAndEtag"], key, numberOfRevisionsToDelete);
            Debug.Assert(numberOfRevisionsToDelete == deletedRevisionsCount);
            IncrementCountOfRevisions(context, key, -deletedRevisionsCount);
        }

        private long IncrementCountOfRevisions(DocumentsOperationContext context, string key, long delta)
        {
            var numbers = context.Transaction.InnerTransaction.ReadTree(VersioningRevisionsCount);
            return numbers.Increment(key, delta);
        }

        private void DeleteCountOfRevisions(DocumentsOperationContext context, string key)
        {
            var numbers = context.Transaction.InnerTransaction.ReadTree(VersioningRevisionsCount);
            numbers.Delete(key);
        }

        public void Delete(DocumentsOperationContext context, string collectionName, string key, Document document, bool isSystemDocument)
        {
            if (isSystemDocument)
                return;

            var configuration = GetVersioningConfiguration(collectionName);
            if (configuration.Active == false)
                return;

            if (configuration.PurgeOnDelete == false)
                return;

            var table = new Table(_docsSchema, "_revisions/" + collectionName, context.Transaction.InnerTransaction);
            table.SeekForwardFrom(_docsSchema.Indexes["KeyAndEtag"], key /* todo, lowered*/, startsWith: true);
        }
    }
}