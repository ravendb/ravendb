// -----------------------------------------------------------------------
//  <copyright file="SnmpDatabase.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using Lextm.SharpSnmpLib.Pipeline;
using Raven.Client.Documents.Changes;
using Raven.Server.Documents;
using Raven.Server.Monitoring.Snmp.Objects.Database;
using Raven.Server.Monitoring.Snmp.Objects.Server;

namespace Raven.Server.Monitoring.Snmp
{
    public class SnmpDatabase
    {
        private readonly ConcurrentDictionary<string, int> _loadedIndexes = new ConcurrentDictionary<string, int>(StringComparer.OrdinalIgnoreCase);

        private readonly DatabasesLandlord _databaseLandlord;

        private readonly ObjectStore _objectStore;

        private readonly string _databaseName;

        private readonly int _databaseIndex;

        private readonly object _locker = new object();

        private bool _attached;

        public SnmpDatabase(DatabasesLandlord databaseLandlord, ObjectStore objectStore, string databaseName, int databaseIndex)
        {
            _databaseLandlord = databaseLandlord;
            _objectStore = objectStore;
            _databaseName = databaseName;
            _databaseIndex = databaseIndex;

            Initialize();

            databaseLandlord.OnDatabaseLoaded += loadedDatabaseName =>
            {
                if (string.Equals(loadedDatabaseName, databaseName, StringComparison.OrdinalIgnoreCase) == false)
                    return;

                Attach(force: true);
            };

            if (databaseLandlord.IsDatabaseLoaded(databaseName))
                Attach(force: false);
        }

        private void Initialize()
        {
            _objectStore.Add(new DatabaseName(_databaseName, _databaseLandlord, _databaseIndex));
            _objectStore.Add(new DatabaseCountOfIndexes(_databaseName, _databaseLandlord, _databaseIndex));
            _objectStore.Add(new DatabaseCountOfStaleIndexes(_databaseName, _databaseLandlord, _databaseIndex));
            _objectStore.Add(new DatabaseCountOfDocuments(_databaseName, _databaseLandlord, _databaseIndex));
            _objectStore.Add(new DatabaseCountOfRevisionDocuments(_databaseName, _databaseLandlord, _databaseIndex));
            _objectStore.Add(new DatabaseCountOfAttachments(_databaseName, _databaseLandlord, _databaseIndex));
            _objectStore.Add(new DatabaseCountOfUniqueAttachments(_databaseName, _databaseLandlord, _databaseIndex));
            _objectStore.Add(new DatabaseAlerts(_databaseName, _databaseLandlord, _databaseIndex));
            _objectStore.Add(new DatabaseId(_databaseName, _databaseLandlord, _databaseIndex));
            _objectStore.Add(new DatabaseUpTime(_databaseName, _databaseLandlord, _databaseIndex));
            _objectStore.Add(new DatabaseLoaded(_databaseName, _databaseLandlord, _databaseIndex));

            //_objectStore.Add(new DatabaseDocsWritePerSecond(_databaseName, _databaseLandlord, _databaseIndex));
            //_objectStore.Add(new DatabaseIndexedPerSecond(_databaseName, _databaseLandlord, _databaseIndex));
            //_objectStore.Add(new DatabaseReducedPerSecond(_databaseName, _databaseLandlord, _databaseIndex));
            //_objectStore.Add(new DatabaseRequestDurationLastMinuteAvg(_databaseName, _databaseLandlord, _databaseIndex));
            //_objectStore.Add(new DatabaseRequestsPerSecond(_databaseName, _databaseLandlord, _databaseIndex));
            //_objectStore.Add(new DatabaseRequestDurationLastMinuteMax(_databaseName, _databaseLandlord, _databaseIndex));
            //_objectStore.Add(new DatabaseRequestDurationLastMinuteMin(_databaseName, _databaseLandlord, _databaseIndex));

            //_objectStore.Add(new DatabaseNumberOfAbandonedIndexes(_databaseName, _databaseLandlord, _databaseIndex));
            //_objectStore.Add(new DatabaseNumberOfAutoIndexes(_databaseName, _databaseLandlord, _databaseIndex));
            //_objectStore.Add(new DatabaseNumberOfDisabledIndexes(_databaseName, _databaseLandlord, _databaseIndex));
            //_objectStore.Add(new DatabaseNumberOfErrorIndexes(_databaseName, _databaseLandlord, _databaseIndex));
            //_objectStore.Add(new DatabaseNumberOfIdleIndexes(_databaseName, _databaseLandlord, _databaseIndex));
            //_objectStore.Add(new DatabaseNumberOfIndexes(_databaseName, _databaseLandlord, _databaseIndex));
            //_objectStore.Add(new DatabaseNumberOfStaticIndexes(_databaseName, _databaseLandlord, _databaseIndex));

            //_objectStore.Add(new DatabaseIndexStorageSize(_databaseName, _databaseLandlord, _databaseIndex));
            //_objectStore.Add(new DatabaseTotalStorageSize(_databaseName, _databaseLandlord, _databaseIndex));
            //_objectStore.Add(new DatabaseTransactionalStorageAllocatedSize(_databaseName, _databaseLandlord, _databaseIndex));
            //_objectStore.Add(new DatabaseTransactionalStorageUsedSize(_databaseName, _databaseLandlord, _databaseIndex));
            //_objectStore.Add(new DatabaseIndexStorageDiskRemainingSpace(_databaseName, _databaseLandlord, _databaseIndex));
            //_objectStore.Add(new DatabaseTransactionalStorageDiskRemainingSpace(_databaseName, _databaseLandlord, _databaseIndex));

            //_objectStore.Add(new ReplicationBundleEnabled(_databaseName, _databaseLandlord, _databaseIndex));

            //AddIndexesFromMappingDocument();
            //AddReplicationDestinationsFromMappingDocument();
        }

        private void Attach(bool force)
        {
            if (force == false && _attached)
                return;

            Task.Factory.StartNew(() =>
            {
                lock (_locker)
                {
                    if (force == false && _attached)
                        return;

                    var database = _databaseLandlord
                        .TryGetOrCreateResourceStore(_databaseName)
                        .Result;

                    database.Changes.OnIndexChange += change =>
                    {
                        if (change.Type != IndexChangeTypes.IndexAdded)
                            return;

                        _loadedIndexes.GetOrAdd(change.Name, AddIndex);
                    };

                    AddIndexesFromDatabase(database);

                    _attached = true;
                }
            });
        }

        private void AddIndexesFromDatabase(DocumentDatabase database)
        {
            foreach (var index in database.IndexStore.GetIndexes())
                _loadedIndexes.GetOrAdd(index.Name, AddIndex);
        }

        private int AddIndex(string indexName)
        {
            //var index = (int)GetOrAddIndex(indexName, MappingDocumentType.Indexes, _databaseLandlord.SystemDatabase);

            //_objectStore.Add(new DatabaseIndexExists(_databaseName, indexName, _databaseLandlord, _databaseIndex, index));
            //_objectStore.Add(new DatabaseIndexName(_databaseName, indexName, _databaseLandlord, _databaseIndex, index));
            //_objectStore.Add(new DatabaseIndexId(_databaseName, indexName, _databaseLandlord, _databaseIndex, index));
            //_objectStore.Add(new DatabaseIndexAttempts(_databaseName, indexName, _databaseLandlord, _databaseIndex, index));
            //_objectStore.Add(new DatabaseIndexErrors(_databaseName, indexName, _databaseLandlord, _databaseIndex, index));
            //_objectStore.Add(new DatabaseIndexPriority(_databaseName, indexName, _databaseLandlord, _databaseIndex, index));
            //_objectStore.Add(new DatabaseIndexAttempts(_databaseName, indexName, _databaseLandlord, _databaseIndex, index));
            //_objectStore.Add(new DatabaseIndexSuccesses(_databaseName, indexName, _databaseLandlord, _databaseIndex, index));
            //_objectStore.Add(new DatabaseIndexReduceAttempts(_databaseName, indexName, _databaseLandlord, _databaseIndex, index));
            //_objectStore.Add(new DatabaseIndexReduceSuccesses(_databaseName, indexName, _databaseLandlord, _databaseIndex, index));
            //_objectStore.Add(new DatabaseIndexReduceErrors(_databaseName, indexName, _databaseLandlord, _databaseIndex, index));
            //_objectStore.Add(new DatabaseIndexTimeSinceLastQuery(_databaseName, indexName, _databaseLandlord, _databaseIndex, index));

            return -1;
        }

        /*
        private void AddIndexesFromMappingDocument()
        {
            var mappingDocument = GetMappingDocument(MappingDocumentType.Indexes);
            if (mappingDocument == null)
                return;

            foreach (var indexName in mappingDocument.DataAsJson.Keys)
                _loadedIndexes.GetOrAdd(indexName, AddIndex);
        }

        private JsonDocument GetMappingDocument(MappingDocumentType type)
        {
            var key = Constants.Monitoring.Snmp.DatabaseMappingDocumentPrefix + _databaseName + "/" + type;

            return _databaseLandlord.SystemDatabase.Documents.Get(key, null);
        }

        private long GetOrAddIndex(string name, MappingDocumentType mappingDocumentType, DocumentDatabase systemDatabase)
        {
            var tries = 0;
            while (true)
            {
                try
                {
                    var key = Constants.Monitoring.Snmp.DatabaseMappingDocumentPrefix + _databaseName + "/" + mappingDocumentType;

                    var mappingDocument = systemDatabase.Documents.Get(key, null) ?? new JsonDocument();

                    RavenJToken value;
                    if (mappingDocument.DataAsJson.TryGetValue(name, out value))
                        return value.Value<int>();

                    var index = 0L;
                    systemDatabase.TransactionalStorage.Batch(actions =>
                    {
                        mappingDocument.DataAsJson[name] = index = actions.General.GetNextIdentityValue(key);
                        systemDatabase.Documents.Put(key, null, mappingDocument.DataAsJson, mappingDocument.Metadata, null);
                    });

                    return index;
                }
                catch (Exception e)
                {
                    Exception _;
                    if (TransactionalStorageHelper.IsWriteConflict(e, out _) == false || tries >= 5)
                        throw;

                    Thread.Sleep(13);
                }
                finally
                {
                    tries++;
                }
            }
        }
        */

        private enum MappingDocumentType
        {
            Indexes,
            Replication
        }
    }
}
