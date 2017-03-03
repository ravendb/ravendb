// -----------------------------------------------------------------------
//  <copyright file="SnmpDatabase.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;

using Lextm.SharpSnmpLib.Pipeline;

using Raven.Abstractions.Data;
using Raven.Abstractions.Replication;
using Raven.Client.Connection;
using Raven.Database.Plugins.Builtins.Monitoring.Snmp.Objects.Database.Bundles.Replication;
using Raven.Database.Plugins.Builtins.Monitoring.Snmp.Objects.Database.Indexes;
using Raven.Database.Plugins.Builtins.Monitoring.Snmp.Objects.Database.Requests;
using Raven.Database.Plugins.Builtins.Monitoring.Snmp.Objects.Database.Statistics;
using Raven.Database.Plugins.Builtins.Monitoring.Snmp.Objects.Database.Storage;
using Raven.Database.Server.Tenancy;
using Raven.Database.Storage;
using Raven.Json.Linq;

namespace Raven.Database.Plugins.Builtins.Monitoring.Snmp
{
    public class SnmpDatabase
    {
        private readonly ConcurrentDictionary<string, int> loadedIndexes = new ConcurrentDictionary<string, int>(StringComparer.OrdinalIgnoreCase);

        private readonly ConcurrentDictionary<string, int> loadedReplicationDestinations = new ConcurrentDictionary<string, int>(StringComparer.OrdinalIgnoreCase);

        private readonly DatabasesLandlord databaseLandlord;

        private readonly ObjectStore store;

        private readonly string databaseName;

        private readonly int databaseIndex;

        private readonly object locker = new object();

        private bool attached;

        public SnmpDatabase(DatabasesLandlord databaseLandlord, ObjectStore store, string databaseName, int databaseIndex)
        {
            this.databaseLandlord = databaseLandlord;
            this.store = store;
            this.databaseName = databaseName;
            this.databaseIndex = databaseIndex;

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
            store.Add(new DatabaseName(databaseName, databaseLandlord, databaseIndex));
            store.Add(new DatabaseApproximateTaskCount(databaseName, databaseLandlord, databaseIndex));
            store.Add(new DatabaseCountOfIndexes(databaseName, databaseLandlord, databaseIndex));
            store.Add(new DatabaseCountOfTransformers(databaseName, databaseLandlord, databaseIndex));
            store.Add(new DatabaseStaleIndexes(databaseName, databaseLandlord, databaseIndex));
            store.Add(new DatabaseCountOfAttachments(databaseName, databaseLandlord, databaseIndex));
            store.Add(new DatabaseCountOfDocuments(databaseName, databaseLandlord, databaseIndex));
            store.Add(new DatabaseCurrentNumberOfItemsToIndexInSingleBatch(databaseName, databaseLandlord, databaseIndex));
            store.Add(new DatabaseCurrentNumberOfItemsToReduceInSingleBatch(databaseName, databaseLandlord, databaseIndex));
            store.Add(new DatabaseErrors(databaseName, databaseLandlord, databaseIndex));
            store.Add(new DatabaseId(databaseName, databaseLandlord, databaseIndex));
            store.Add(new DatabaseActiveBundles(databaseName, databaseLandlord, databaseIndex));
            store.Add(new DatabaseLoaded(databaseName, databaseLandlord, databaseIndex));

            store.Add(new DatabaseDocsWritePerSecond(databaseName, databaseLandlord, databaseIndex));
            store.Add(new DatabaseIndexedPerSecond(databaseName, databaseLandlord, databaseIndex));
            store.Add(new DatabaseReducedPerSecond(databaseName, databaseLandlord, databaseIndex));
            store.Add(new DatabaseRequestDurationLastMinuteAvg(databaseName, databaseLandlord, databaseIndex));
            store.Add(new DatabaseRequestsPerSecond(databaseName, databaseLandlord, databaseIndex));
            store.Add(new DatabaseRequestDurationLastMinuteMax(databaseName, databaseLandlord, databaseIndex));
            store.Add(new DatabaseRequestDurationLastMinuteMin(databaseName, databaseLandlord, databaseIndex));

            store.Add(new DatabaseNumberOfAbandonedIndexes(databaseName, databaseLandlord, databaseIndex));
            store.Add(new DatabaseNumberOfAutoIndexes(databaseName, databaseLandlord, databaseIndex));
            store.Add(new DatabaseNumberOfDisabledIndexes(databaseName, databaseLandlord, databaseIndex));
            store.Add(new DatabaseNumberOfErrorIndexes(databaseName, databaseLandlord, databaseIndex));
            store.Add(new DatabaseNumberOfIdleIndexes(databaseName, databaseLandlord, databaseIndex));
            store.Add(new DatabaseNumberOfIndexes(databaseName, databaseLandlord, databaseIndex));
            store.Add(new DatabaseNumberOfStaticIndexes(databaseName, databaseLandlord, databaseIndex));

            store.Add(new DatabaseIndexStorageSize(databaseName, databaseLandlord, databaseIndex));
            store.Add(new DatabaseTotalStorageSize(databaseName, databaseLandlord, databaseIndex));
            store.Add(new DatabaseTransactionalStorageAllocatedSize(databaseName, databaseLandlord, databaseIndex));
            store.Add(new DatabaseTransactionalStorageUsedSize(databaseName, databaseLandlord, databaseIndex));
            store.Add(new DatabaseIndexStorageDiskRemainingSpace(databaseName, databaseLandlord, databaseIndex));
            store.Add(new DatabaseTransactionalStorageDiskRemainingSpace(databaseName, databaseLandlord, databaseIndex));

            store.Add(new ReplicationBundleEnabled(databaseName, databaseLandlord, databaseIndex));

            AddIndexesFromMappingDocument();
            AddReplicationDestinationsFromMappingDocument();
        }

        private void Attach(bool force)
        {
            if (force == false && attached)
                return;

            Task.Factory.StartNew(() =>
            {
                lock (locker)
                {
                    if (force == false && attached)
                        return;

                    var database = databaseLandlord
                        .GetResourceInternal(databaseName)
                        .Result;

                    database.Notifications.OnIndexChange += (db, notification) =>
                    {
                        if (notification.Type != IndexChangeTypes.IndexAdded)
                            return;

                        loadedIndexes.GetOrAdd(notification.Name, AddIndex);
                    };

                    database.ConfigurationRetriever.SubscribeToConfigurationDocumentChanges(Constants.RavenReplicationDestinations, AddReplicationDestinationsFromDatabase);

                    AddIndexesFromDatabase(database);
                    AddReplicationDestinationsFromDatabase();

                    attached = true;
                }
            });
        }

        private void AddReplicationDestinationsFromDatabase()
        {
            if (databaseLandlord.IsDatabaseLoaded(databaseName) == false)
                return;

            var database = databaseLandlord.GetResourceInternal(databaseName).Result;

            var replicationDocument = database.ConfigurationRetriever.GetConfigurationDocument<ReplicationDocument<ReplicationDestination.ReplicationDestinationWithConfigurationOrigin>>(Constants.RavenReplicationDestinations);
            if (replicationDocument == null)
                return;

            foreach (var destination in replicationDocument.MergedDocument.Destinations)
                loadedReplicationDestinations.GetOrAdd(destination.Url.ForDatabase(destination.Database), AddReplicationDestination);
        }

        private int AddReplicationDestination(string replicationDestinationUrl)
        {
            var index = (int)GetOrAddIndex(replicationDestinationUrl, MappingDocumentType.Replication, databaseLandlord.SystemDatabase);

            store.Add(new ReplicationDestinationEnabled(databaseName, databaseLandlord, databaseIndex, replicationDestinationUrl, index));
            store.Add(new ReplicationDestinationUrl(databaseName, databaseLandlord, databaseIndex, replicationDestinationUrl, index));
            store.Add(new ReplicationDestinationTimeSinceLastReplication(databaseName, databaseLandlord, databaseIndex, replicationDestinationUrl, index));

            return index;
        }

        private void AddIndexesFromDatabase(DocumentDatabase database)
        {
            var indexes = database.IndexStorage.IndexNames;

            foreach (var indexName in indexes)
                loadedIndexes.GetOrAdd(indexName, AddIndex);
        }

        private int AddIndex(string indexName)
        {
            var index = (int)GetOrAddIndex(indexName, MappingDocumentType.Indexes, databaseLandlord.SystemDatabase);

            store.Add(new DatabaseIndexExists(databaseName, indexName, databaseLandlord, databaseIndex, index));
            store.Add(new DatabaseIndexName(databaseName, indexName, databaseLandlord, databaseIndex, index));
            store.Add(new DatabaseIndexId(databaseName, indexName, databaseLandlord, databaseIndex, index));
            store.Add(new DatabaseIndexAttempts(databaseName, indexName, databaseLandlord, databaseIndex, index));
            store.Add(new DatabaseIndexErrors(databaseName, indexName, databaseLandlord, databaseIndex, index));
            store.Add(new DatabaseIndexPriority(databaseName, indexName, databaseLandlord, databaseIndex, index));
            store.Add(new DatabaseIndexAttempts(databaseName, indexName, databaseLandlord, databaseIndex, index));
            store.Add(new DatabaseIndexSuccesses(databaseName, indexName, databaseLandlord, databaseIndex, index));
            store.Add(new DatabaseIndexReduceAttempts(databaseName, indexName, databaseLandlord, databaseIndex, index));
            store.Add(new DatabaseIndexReduceSuccesses(databaseName, indexName, databaseLandlord, databaseIndex, index));
            store.Add(new DatabaseIndexReduceErrors(databaseName, indexName, databaseLandlord, databaseIndex, index));
            store.Add(new DatabaseIndexTimeSinceLastQuery(databaseName, indexName, databaseLandlord, databaseIndex, index));

            return index;
        }

        private void AddIndexesFromMappingDocument()
        {
            var mappingDocument = GetMappingDocument(MappingDocumentType.Indexes);
            if (mappingDocument == null)
                return;

            foreach (var indexName in mappingDocument.DataAsJson.Keys)
                loadedIndexes.GetOrAdd(indexName, AddIndex);
        }

        private void AddReplicationDestinationsFromMappingDocument()
        {
            var mappingDocument = GetMappingDocument(MappingDocumentType.Replication);
            if (mappingDocument == null)
                return;

            foreach (var replicationDestinationUrl in mappingDocument.DataAsJson.Keys)
                loadedReplicationDestinations.GetOrAdd(replicationDestinationUrl, AddReplicationDestination);
        }

        private JsonDocument GetMappingDocument(MappingDocumentType type)
        {
            var key = Constants.Monitoring.Snmp.DatabaseMappingDocumentPrefix + databaseName + "/" + type;

            return databaseLandlord.SystemDatabase.Documents.Get(key, null);
        }

        private long GetOrAddIndex(string name, MappingDocumentType mappingDocumentType, DocumentDatabase systemDatabase)
        {
            var tries = 0;
            while (true)
            {
                try
                {
                    var key = Constants.Monitoring.Snmp.DatabaseMappingDocumentPrefix + databaseName + "/" + mappingDocumentType;

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

        private enum MappingDocumentType
        {
            Indexes,
            Replication
        }
    }
}
