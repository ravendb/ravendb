using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Archival;
using Raven.Client.ServerWide;
using Raven.Server.Background;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Logging;
using Sparrow.Platform;
using Voron;

namespace Raven.Server.Documents.Archival
{
    public class DataArchivist : BackgroundWorkBase
    {
        internal static int BatchSize = PlatformDetails.Is32Bits == false
            ? 4096
            : 1024;

        private readonly DocumentDatabase _database;
        private readonly TimeSpan _archivePeriod;

        public ArchivalConfiguration ArchivalConfiguration { get; }

        private DataArchivist(DocumentDatabase database, ArchivalConfiguration archivalConfiguration) : base(database.Name, database.DatabaseShutdown)
        {
            ArchivalConfiguration = archivalConfiguration;
            _database = database;
            _archivePeriod = TimeSpan.FromSeconds(ArchivalConfiguration?.ArchiveFrequencyInSec ?? 60);
        }

        public static DataArchivist LoadConfigurations(DocumentDatabase database, DatabaseRecord dbRecord, DataArchivist dataArchivist)
        {
            try
            {
                if (dbRecord.Archival == null)
                {
                    dataArchivist?.Dispose();
                    return null;
                }

                if (dataArchivist != null)
                {
                    // no changes
                    if (Equals(dataArchivist.ArchivalConfiguration, dbRecord.Archival))
                        return dataArchivist;
                }

                dataArchivist?.Dispose();

                var hasArchive = dbRecord.Archival?.Disabled == false;

                if (hasArchive == false)
                    return null;

                var archiver = new DataArchivist(database, dbRecord.Archival);
                archiver.Start();
                return archiver;
            }
            catch (Exception e)
            {
                const string msg = "Cannot enable documents archiver as the configuration record is not valid.";
                database.NotificationCenter.Add(AlertRaised.Create(
                    database.Name,
                    $"Archive error in {database.Name}", msg,
                    AlertType.RevisionsConfigurationNotValid, NotificationSeverity.Error, database.Name));

                var logger = LoggingSource.Instance.GetLogger<DataArchivist>(database.Name);
                if (logger.IsOperationsEnabled)
                    logger.Operations(msg, e);

                return null;
            }
        }

        protected override Task DoWork()
        {
            return DoArchiveWork();
        }

        private async Task DoArchiveWork()
        {
            while (ArchivalConfiguration?.Disabled == false)
            {
                await WaitOrThrowOperationCanceled(_archivePeriod);

                await ArchiveDocs();
            }
        }

        internal Task ArchiveDocs(int? batchSize = null)
        {
            return ArchiveDocs(batchSize ?? BatchSize);
        }
        
        private async Task ArchiveDocs(int batchSize)
        {
            var currentTime = _database.Time.GetUtcNow();
            
            try
            {
                // DatabaseTopology topology;
                // using (_database.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext serverContext))
                // using (serverContext.OpenReadTransaction())
                // {
                //     topology = _database.ServerStore.Cluster.ReadDatabaseTopology(serverContext, _database.Name);
                // }
                //
                // var isFirstInTopology = string.Equals(topology.AllNodes.FirstOrDefault(), _database.ServerStore.NodeTag, StringComparison.OrdinalIgnoreCase);

                using (_database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                {
                    while (true)
                    {
                        context.Reset();
                        context.Renew();

                        using (context.OpenReadTransaction())
                        {
                            var options = new ArchivalStorage.ArchivedDocumentsOptions(context, currentTime, batchSize);

                            var toArchive =_database.DocumentsStorage.ArchivalStorage.GetDocumentsToArchive(options, out var duration, CancellationToken);

                            if (toArchive == null || toArchive.Count == 0)
                                return;

                            var command = new ArchiveDocumentsCommand(toArchive, _database, currentTime);
                            await _database.TxMerger.Enqueue(command);

                            if (Logger.IsInfoEnabled)
                                Logger.Info($"Successfully archived {command.DeletionCount:#,#;;0} documents in {duration.ElapsedMilliseconds:#,#;;0} ms.");
                        }
                    }
                }
            }
            catch (OperationCanceledException)
            {
                // this will stop processing
                throw;
            }
            catch (Exception e)
            {
                if (Logger.IsOperationsEnabled)
                    Logger.Operations($"Failed to archive documents on {_database.Name} which are older than {currentTime}", e);
            }
        }

        internal class ArchiveDocumentsCommand: MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>
        {
            private readonly Dictionary<Slice, List<(Slice LowerId, string Id)>> _toArchive;
            private readonly DocumentDatabase _database;
            private readonly DateTime _currentTime;

            public int DeletionCount;

            public ArchiveDocumentsCommand(Dictionary<Slice, List<(Slice LowerId, string Id)>> toArchive, DocumentDatabase database, DateTime currentTime)
            {
                _toArchive = toArchive;
                _database = database;
                _currentTime = currentTime;
            }

            protected override long ExecuteCmd(DocumentsOperationContext context)
            {
                DeletionCount = _database.DocumentsStorage.ArchivalStorage.ArchiveDocuments(context, _toArchive, _currentTime);
                return DeletionCount;
            }

            public override IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, DataArchivist.ArchiveDocumentsCommand> ToDto(DocumentsOperationContext context)
            {
                var keyValuePairs = new KeyValuePair<Slice, List<(Slice LowerId, string Id)>>[_toArchive.Count];
                var i = 0;
                foreach (var item in _toArchive)
                {
                    keyValuePairs[i] = item;
                    i++;
                }

                return new ArchiveDocumentsCommandDto 
                {
                    Expired = keyValuePairs,
                    CurrentTime = _currentTime
                };
            }
        }
    }

    internal class ArchiveDocumentsCommandDto: IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, DataArchivist.ArchiveDocumentsCommand>
    {
        public DataArchivist.ArchiveDocumentsCommand ToCommand(DocumentsOperationContext context, DocumentDatabase database)
        {
            var toArchive = new Dictionary<Slice, List<(Slice LowerId, string Id)>>();
            foreach (var item in Expired)
            {
                toArchive[item.Key] = item.Value;
            }
            var command = new DataArchivist.ArchiveDocumentsCommand(toArchive, database, CurrentTime);
            return command;
        }

        public KeyValuePair<Slice, List<(Slice LowerId, string Id)>>[] Expired { get; set; }

        public DateTime CurrentTime { get; set; }
    }
}
