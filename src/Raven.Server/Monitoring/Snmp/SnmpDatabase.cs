using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Lextm.SharpSnmpLib.Pipeline;
using Raven.Client.Documents.Changes;
using Raven.Client.Exceptions.Database;
using Raven.Client.Util;
using Raven.Client.ServerWide;
using Raven.Server.Documents;
using Raven.Server.Monitoring.Snmp.Objects.Database;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands.Monitoring.Snmp;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Monitoring.Snmp
{
    public sealed class SnmpDatabase
    {
        private readonly Dictionary<string, int> _loadedIndexes = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        private readonly Dictionary<string, int> _loadedEtls = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        private readonly Dictionary<string, int> _loadedAiTasks = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);

        private readonly DatabasesLandlord _databaseLandlord;

        private readonly ObjectStore _objectStore;

        private readonly string _databaseName;

        private readonly int _databaseIndex;

        private readonly SemaphoreSlim _locker = new SemaphoreSlim(1, 1);

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
            _objectStore.Add(new DatabaseRehabs(_databaseName, _databaseLandlord, _databaseIndex));
            _objectStore.Add(new DatabasePerformanceHints(_databaseName, _databaseLandlord, _databaseIndex));
            _objectStore.Add(new DatabaseIndexingErrors(_databaseName, _databaseLandlord, _databaseIndex));
            _objectStore.Add(new DatabaseEtlErrors(_databaseName, _databaseLandlord, _databaseIndex));
            _objectStore.Add(new DatabaseAiTaskErrors(_databaseName, _databaseLandlord, _databaseIndex));

            _objectStore.Add(new DatabaseDocPutsPerSecond(_databaseName, _databaseLandlord, _databaseIndex));
            _objectStore.Add(new DatabaseMapIndexIndexedPerSecond(_databaseName, _databaseLandlord, _databaseIndex));
            _objectStore.Add(new DatabaseMapReduceIndexMappedPerSecond(_databaseName, _databaseLandlord, _databaseIndex));
            _objectStore.Add(new DatabaseMapReduceIndexReducedPerSecond(_databaseName, _databaseLandlord, _databaseIndex));
            _objectStore.Add(new DatabaseRequestsPerSecond(_databaseName, _databaseLandlord, _databaseIndex));
            _objectStore.Add(new DatabaseRequestsCount(_databaseName, _databaseLandlord, _databaseIndex));
            _objectStore.Add(new DatabaseAverageRequestTime(_databaseName, _databaseLandlord, _databaseIndex));

            _objectStore.Add(new DatabaseNumberOfAutoIndexes(_databaseName, _databaseLandlord, _databaseIndex));
            _objectStore.Add(new DatabaseNumberOfDisabledIndexes(_databaseName, _databaseLandlord, _databaseIndex));
            _objectStore.Add(new DatabaseNumberOfErrorIndexes(_databaseName, _databaseLandlord, _databaseIndex));
            _objectStore.Add(new DatabaseNumberOfFaultyIndexes(_databaseName, _databaseLandlord, _databaseIndex));
            _objectStore.Add(new DatabaseNumberOfIdleIndexes(_databaseName, _databaseLandlord, _databaseIndex));
            _objectStore.Add(new DatabaseNumberOfIndexes(_databaseName, _databaseLandlord, _databaseIndex));
            _objectStore.Add(new DatabaseNumberOfStaticIndexes(_databaseName, _databaseLandlord, _databaseIndex));

            _objectStore.Add(new DatabaseDocumentsStorageAllocatedSize(_databaseName, _databaseLandlord, _databaseIndex));
            _objectStore.Add(new DatabaseDocumentsStorageUsedSize(_databaseName, _databaseLandlord, _databaseIndex));
            _objectStore.Add(new DatabaseIndexStorageAllocatedSize(_databaseName, _databaseLandlord, _databaseIndex));
            _objectStore.Add(new DatabaseIndexStorageUsedSize(_databaseName, _databaseLandlord, _databaseIndex));
            _objectStore.Add(new DatabaseTotalStorageSize(_databaseName, _databaseLandlord, _databaseIndex));
            _objectStore.Add(new DatabaseStorageDiskRemainingSpace(_databaseName, _databaseLandlord, _databaseIndex));
            _objectStore.Add(new DatabaseStorageDiskIosReadOperations(_databaseName, _databaseLandlord, _databaseIndex));
            _objectStore.Add(new DatabaseStorageDiskIosWriteOperations(_databaseName, _databaseLandlord, _databaseIndex));
            _objectStore.Add(new DatabaseStorageDiskReadThroughput(_databaseName, _databaseLandlord, _databaseIndex));
            _objectStore.Add(new DatabaseStorageDiskWriteThroughput(_databaseName, _databaseLandlord, _databaseIndex));
            _objectStore.Add(new DatabaseStorageDiskQueueLength(_databaseName, _databaseLandlord, _databaseIndex));

            _objectStore.Add(new DatabaseWritesPerSecond(_databaseName, _databaseLandlord, _databaseIndex));
            _objectStore.Add(new DatabaseDataWrittenPerSecond(_databaseName, _databaseLandlord, _databaseIndex));
            
            _objectStore.Add(new DatabaseHealthyEtls(_databaseName, _databaseLandlord, _databaseIndex));
            _objectStore.Add(new DatabaseImpairedEtls(_databaseName, _databaseLandlord, _databaseIndex));
            _objectStore.Add(new DatabaseFailedEtls(_databaseName, _databaseLandlord, _databaseIndex));
            _objectStore.Add(new DatabaseTotalNumberOfEtls(_databaseName, _databaseLandlord, _databaseIndex));
            _objectStore.Add(new DatabaseActiveEtls(_databaseName, _databaseLandlord, _databaseIndex));
            _objectStore.Add(new DatabaseEtlTotalDocumentsProcessedPerSec(_databaseName, _databaseLandlord, _databaseIndex));

            _objectStore.Add(new DatabaseHealthyAiTasks(_databaseName, _databaseLandlord, _databaseIndex));
            _objectStore.Add(new DatabaseImpairedAiTasks(_databaseName, _databaseLandlord, _databaseIndex));
            _objectStore.Add(new DatabaseFailedAiTasks(_databaseName, _databaseLandlord, _databaseIndex));
            _objectStore.Add(new DatabaseTotalNumberOfAiTasks(_databaseName, _databaseLandlord, _databaseIndex));
            _objectStore.Add(new DatabaseActiveAiTasks(_databaseName, _databaseLandlord, _databaseIndex));
            _objectStore.Add(new DatabaseAiTaskTotalDocumentsProcessedPerSec(_databaseName, _databaseLandlord, _databaseIndex));

            //AddIndexesFromMappingDocument();
        }

        private void Attach(bool force)
        {
            if (force == false && _attached)
                return;

            Task.Factory.StartNew(async () =>
            {
                await _locker.WaitAsync();

                try
                {
                    if (force == false && _attached)
                        return;

                    var database = await _databaseLandlord.TryGetOrCreateResourceStore(_databaseName);

                    database.Changes.OnIndexChange += AddIndexIfNecessary;
                    database.DatabaseRecordChanged += OnDatabaseRecordChanged;

                    await AddIndexesFromDatabaseAsync(database);
                    await AddEtlsFromDatabaseAsync(database);
                    await AddAiTasksFromDatabaseAsync(database);

                    _attached = true;
                }
                catch (DatabaseDisabledException)
                {
                    // ignored
                }
                finally
                {
                    _locker.Release();
                }
            });
        }

        private void AddIndexIfNecessary(IndexChange change)
        {
            if (change.Type != IndexChangeTypes.IndexAdded)
                return;

            Task.Factory.StartNew(async () =>
            {
                await _locker.WaitAsync();

                try
                {
                    if (_loadedIndexes.ContainsKey(change.Name))
                        return;

                    var database = await _databaseLandlord.TryGetOrCreateResourceStore(_databaseName);

                    using (database.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                    {
                        context.OpenReadTransaction();

                        var mapping = GetIndexMapping(context, database.ServerStore, database.Name);
                        if (mapping.ContainsKey(change.Name) == false)
                        {
                            context.CloseTransaction();

                            var result = await database.ServerStore.SendToLeaderAsync(new UpdateSnmpDatabaseIndexesMappingCommand(_databaseName, new List<string>
                            {
                                change.Name
                            }, RaftIdGenerator.NewId()));

                            await database.ServerStore.Cluster.WaitForIndexNotification(result.Index);

                            context.OpenReadTransaction();

                            mapping = GetIndexMapping(context, database.ServerStore, database.Name);
                        }

                        LoadIndex(change.Name, (int)mapping[change.Name]);
                    }
                }
                finally
                {
                    _locker.Release();
                }
            });
        }

        private void OnDatabaseRecordChanged(DatabaseRecord record)
        {
            Task.Factory.StartNew(async () =>
            {
                await _locker.WaitAsync();

                try
                {
                    var database = await _databaseLandlord.TryGetOrCreateResourceStore(_databaseName);

                    await AddEtlsFromDatabaseAsync(database);
                    await AddAiTasksFromDatabaseAsync(database);
                }
                finally
                {
                    _locker.Release();
                }
            });
        }

        private async Task AddIndexesFromDatabaseAsync(DocumentDatabase database)
        {
            var indexes = database.IndexStore.GetIndexes().ToList();

            if (indexes.Count == 0)
                return;

            using (database.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                context.OpenReadTransaction();

                var mapping = GetIndexMapping(context, database.ServerStore, database.Name);

                var missingIndexes = new List<string>();
                foreach (var index in indexes)
                {
                    if (mapping.ContainsKey(index.Name) == false)
                        missingIndexes.Add(index.Name);
                }

                if (missingIndexes.Count > 0)
                {
                    context.CloseTransaction();

                    var result = await database.ServerStore.SendToLeaderAsync(new UpdateSnmpDatabaseIndexesMappingCommand(database.Name, missingIndexes, RaftIdGenerator.NewId()));
                    await database.ServerStore.Cluster.WaitForIndexNotification(result.Index);

                    context.OpenReadTransaction();

                    mapping = GetIndexMapping(context, database.ServerStore, database.Name);
                }

                foreach (var index in indexes)
                    LoadIndex(index.Name, (int)mapping[index.Name]);
            }
        }
        
        private async Task AddEtlsFromDatabaseAsync(DocumentDatabase database)
        {
            var etlNames = database.EtlLoader.GetEtlProcessNamesFromRecord().ToList();

            if (etlNames.Count == 0)
                return;

            using (database.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                context.OpenReadTransaction();

                var mapping = GetEtlMapping(context, database.ServerStore, database.Name);

                var missingEtls = new List<string>();
                foreach (var etlName in etlNames)
                {
                    if (mapping.ContainsKey(etlName) == false)
                        missingEtls.Add(etlName);
                }

                if (missingEtls.Count > 0)
                {
                    context.CloseTransaction();

                    var result = await database.ServerStore.SendToLeaderAsync(new UpdateSnmpDatabaseEtlsMappingCommand(database.Name, missingEtls, RaftIdGenerator.NewId()));
                    await database.ServerStore.Cluster.WaitForIndexNotification(result.Index);

                    context.OpenReadTransaction();

                    mapping = GetEtlMapping(context, database.ServerStore, database.Name);
                }

                foreach (var etlName in etlNames)
                    LoadEtl(etlName, (int)mapping[etlName]);
            }
        }

        private async Task AddAiTasksFromDatabaseAsync(DocumentDatabase database)
        {
            var aiTaskNames = database.EtlLoader.GetAiProcessNamesFromRecord().ToList();

            if (aiTaskNames.Count == 0)
                return;

            using (database.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                context.OpenReadTransaction();

                var mapping = GetAiTasksMapping(context, database.ServerStore, database.Name);

                var missingAiTasks = new List<string>();
                foreach (var aiTaskName in aiTaskNames)
                {
                    if (mapping.ContainsKey(aiTaskName) == false)
                        missingAiTasks.Add(aiTaskName);
                }

                if (missingAiTasks.Count > 0)
                {
                    context.CloseTransaction();

                    var result = await database.ServerStore.SendToLeaderAsync(new UpdateSnmpDatabaseAiTasksMappingCommand(database.Name, missingAiTasks, RaftIdGenerator.NewId()));
                    await database.ServerStore.Cluster.WaitForIndexNotification(result.Index);

                    context.OpenReadTransaction();

                    mapping = GetAiTasksMapping(context, database.ServerStore, database.Name);
                }

                foreach (var aiTaskName in aiTaskNames)
                    LoadAiTask(aiTaskName, (int)mapping[aiTaskName]);
            }
        }

        private void LoadIndex(string indexName, int index)
        {
            if (_loadedIndexes.ContainsKey(indexName))
                return;

            _objectStore.Add(new DatabaseIndexExists(_databaseName, indexName, _databaseLandlord, _databaseIndex, index));
            _objectStore.Add(new DatabaseIndexName(_databaseName, indexName, _databaseLandlord, _databaseIndex, index));
            _objectStore.Add(new DatabaseIndexPriority(_databaseName, indexName, _databaseLandlord, _databaseIndex, index));
            _objectStore.Add(new DatabaseIndexState(_databaseName, indexName, _databaseLandlord, _databaseIndex, index));
            _objectStore.Add(new DatabaseIndexErrors(_databaseName, indexName, _databaseLandlord, _databaseIndex, index));
            _objectStore.Add(new DatabaseIndexLastQueryTime(_databaseName, indexName, _databaseLandlord, _databaseIndex, index));
            _objectStore.Add(new DatabaseIndexLastIndexingTime(_databaseName, indexName, _databaseLandlord, _databaseIndex, index));
            _objectStore.Add(new DatabaseIndexTimeSinceLastQuery(_databaseName, indexName, _databaseLandlord, _databaseIndex, index));
            _objectStore.Add(new DatabaseIndexTimeSinceLastIndexing(_databaseName, indexName, _databaseLandlord, _databaseIndex, index));
            _objectStore.Add(new DatabaseIndexLockMode(_databaseName, indexName, _databaseLandlord, _databaseIndex, index));
            _objectStore.Add(new DatabaseIndexIsInvalid(_databaseName, indexName, _databaseLandlord, _databaseIndex, index));
            _objectStore.Add(new DatabaseIndexStatus(_databaseName, indexName, _databaseLandlord, _databaseIndex, index));
            _objectStore.Add(new DatabaseIndexMapsPerSec(_databaseName, indexName, _databaseLandlord, _databaseIndex, index));
            _objectStore.Add(new DatabaseIndexReducesPerSec(_databaseName, indexName, _databaseLandlord, _databaseIndex, index));
            _objectStore.Add(new DatabaseIndexType(_databaseName, indexName, _databaseLandlord, _databaseIndex, index));

            _loadedIndexes[indexName] = index;
        }
        
        private void LoadEtl(string processName, int index)
        {
            if (_loadedEtls.ContainsKey(processName))
                return;

            _objectStore.Add(new DatabaseEtlErrorsOfTask(_databaseName, processName, _databaseLandlord, _databaseIndex, index));
            _objectStore.Add(new DatabaseEtlHealthStatus(_databaseName, processName, _databaseLandlord, _databaseIndex, index));
            _objectStore.Add(new DatabaseEtlLastSuccessfulBatchTime(_databaseName, processName, _databaseLandlord, _databaseIndex, index));
            _objectStore.Add(new DatabaseEtlDocumentsProcessedPerSec(_databaseName, processName, _databaseLandlord, _databaseIndex, index));
            _objectStore.Add(new DatabaseEtlTaskResponsibleNode(_databaseName, processName, _databaseLandlord, _databaseIndex, index));

            _loadedEtls[processName] = index;
        }

        private void LoadAiTask(string processName, int index)
        {
            if (_loadedAiTasks.ContainsKey(processName))
                return;

            _objectStore.Add(new DatabaseErrorsOfAiTask(_databaseName, processName, _databaseLandlord, _databaseIndex, index));
            _objectStore.Add(new DatabaseAiTaskHealthStatus(_databaseName, processName, _databaseLandlord, _databaseIndex, index));
            _objectStore.Add(new DatabaseAiTaskLastSuccessfulBatchTime(_databaseName, processName, _databaseLandlord, _databaseIndex, index));
            _objectStore.Add(new DatabaseAiTaskDocumentsProcessedPerSec(_databaseName, processName, _databaseLandlord, _databaseIndex, index));
            _objectStore.Add(new DatabaseAiTaskResponsibleNode(_databaseName, processName, _databaseLandlord, _databaseIndex, index));

            _loadedAiTasks[processName] = index;
        }

        internal static Dictionary<string, long> GetIndexMapping(TransactionOperationContext context, ServerStore serverStore, string databaseName)
        {
            return GetMapping(context, serverStore, UpdateSnmpDatabaseIndexesMappingCommand.GetStorageKey(databaseName));
        }

        internal static Dictionary<string, long> GetEtlMapping(TransactionOperationContext context, ServerStore serverStore, string databaseName)
        {
            return GetMapping(context, serverStore, UpdateSnmpDatabaseEtlsMappingCommand.GetStorageKey(databaseName));
        }

        internal static Dictionary<string, long> GetAiTasksMapping(TransactionOperationContext context, ServerStore serverStore, string databaseName)
        {
            return GetMapping(context, serverStore, UpdateSnmpDatabaseAiTasksMappingCommand.GetStorageKey(databaseName));
        }

        private static Dictionary<string, long> GetMapping(TransactionOperationContext context, ServerStore serverStore, string storageKey)
        {
            var json = serverStore.Cluster.Read(context, storageKey);

            var result = new Dictionary<string, long>(StringComparer.OrdinalIgnoreCase);
            if (json == null)
                return result;

            var propertyDetails = new BlittableJsonReaderObject.PropertyDetails();
            for (int i = 0; i < json.Count; i++)
            {
                json.GetPropertyByIndex(i, ref propertyDetails);

                result[propertyDetails.Name] = (long)propertyDetails.Value;
            }

            return result;
        }
    }
}
