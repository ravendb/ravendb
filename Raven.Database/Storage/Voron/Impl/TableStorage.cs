// -----------------------------------------------------------------------
//  <copyright file="TableStorage.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Threading;
using System.Web.UI;
using Mono.CSharp;
using Raven.Abstractions.Util.Streams;
using Raven.Database.Indexing.Collation.Cultures;
using Raven.Database.Storage.Voron.StorageActions.StructureSchemas;
using Voron.Impl.Paging;
using Voron.Trees;

namespace Raven.Database.Storage.Voron.Impl
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;

    using global::Voron;
    using global::Voron.Debugging;
    using global::Voron.Impl;

    internal class TableStorage : IDisposable
    {
        private readonly StorageEnvironmentOptions _options;
        private readonly IBufferPool bufferPool;

        private readonly StorageEnvironment env;

#if DEBUG
        public TableStorage(StorageEnvironment environment, IBufferPool bufferPool)
        {
            this.bufferPool = bufferPool;
            env = environment;

            Initialize();
        }
#endif

        public TableStorage(StorageEnvironmentOptions options, IBufferPool bufferPool)
        {
            if (options == null)
                throw new ArgumentNullException("options");

            _options = options;
            this.bufferPool = bufferPool;

            Debug.Assert(options != null);

//#if DEBUG
//			var directoryOptions = options as StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions;
//
//			string debugJournalName;
//			if (directoryOptions != null)
//				debugJournalName = directoryOptions.TempPath.Replace(Path.DirectorySeparatorChar, '_').Replace(':','_');
//			else
//				debugJournalName = "InMemoryDebugJournal-" + Interlocked.Increment(ref debugJournalCount);
//
//			env = new StorageEnvironment(options, debugJournalName) {IsDebugRecording = true};
//#else
            env = new StorageEnvironment(options);
//#endif
            Initialize();
        }

        internal StorageReport GenerateReportOnStorage(bool computeExactSizes, Action<string> progress, CancellationToken token)
        {
            using (var tran = env.NewTransaction(TransactionFlags.Read))
            {
                return env.GenerateReport(tran, computeExactSizes, progress, token);
            }
        }

        public SnapshotReader CreateSnapshot()
        {
            return env.CreateSnapshot();
        }

        public Table Details { get; private set; }

        public Table Documents { get; private set; }

        public TableOfStructures<IndexingWorkStatsFields> IndexingStats { get; private set; }

        public TableOfStructures<ReducingWorkStatsFields> ReduceStats { get; private set; }

        public Table IndexingMetadata { get; private set; }

        public TableOfStructures<LastIndexedStatsFields> LastIndexedEtags { get; private set; }

        public TableOfStructures<DocumentReferencesFields> DocumentReferences { get; private set; }

        public Table Queues { get; private set; }

        public Table Lists { get; private set; }

        public TableOfStructures<TaskFields> Tasks { get; private set; }

        public TableOfStructures<ScheduledReductionFields> ScheduledReductions { get; private set; }

        public TableOfStructures<MappedResultFields> MappedResults { get; private set; }

        public TableOfStructures<ReduceResultFields> ReduceResults { get; private set; }

        [Obsolete("Use RavenFS instead.")]
        public Table Attachments { get; private set; }

        public TableOfStructures<ReduceKeyCountFields> ReduceKeyCounts { get; private set; }

        public TableOfStructures<ReduceKeyTypeFields> ReduceKeyTypes { get; private set; }

        public Table General { get; private set; }

        public StorageEnvironment Environment
        {
            get
            {
                return env;
            }
        }

        public void Write(WriteBatch writeBatch)
        {
            try
            {
                env.Writer.Write(writeBatch);
            }
            catch (AggregateException ae)
            {
                if (ae.InnerException is OperationCanceledException == false) // this can happen during storage disposal
                    throw;
            }
        }

        public long GetEntriesCount(TableBase table)
        {
            using (var tx = env.NewTransaction(TransactionFlags.Read))
            {
                return tx.ReadTree(table.TableName).State.EntriesCount;
            }
        }

        public void RenderAndShow(TableBase table, int showEntries = 25)
        {
            if (Debugger.IsAttached == false)
                return;

            using (var tx = env.NewTransaction(TransactionFlags.Read))
            {
                RenderAndShow(tx, table, showEntries);
            }
        }

        public void RenderAndShow(Transaction tx, TableBase table, int showEntries = 25)
        {
            if (Debugger.IsAttached == false)
                return;

            var tree = env.CreateTree(tx, table.TableName);

            var path = Path.Combine(System.Environment.CurrentDirectory, "test-tree.dot");
            var rootPageNumber = tree.State.RootPageNumber;
            TreeDumper.Dump(tx, path, tx.GetReadOnlyPage(rootPageNumber), showEntries);

            var output = Path.Combine(System.Environment.CurrentDirectory, "output.svg");
            var p = Process.Start(FindGraphviz() + @"\bin\dot.exe", "-Tsvg " + path + " -o " + output);
            p.WaitForExit();
            Process.Start(output);
        }

        public void Dispose()
        {
            if (env != null)
                env.Dispose();
        }

        private void Initialize()
        {
            Documents = new Table(Tables.Documents.TableName, bufferPool, Tables.Documents.Indices.KeyByEtag, Tables.Documents.Indices.Metadata);
            Details = new Table(Tables.Details.TableName, bufferPool);
            IndexingStats = new TableOfStructures<IndexingWorkStatsFields>(Tables.IndexingStats.TableName,
                new StructureSchema<IndexingWorkStatsFields>()
                    .Add<int>(IndexingWorkStatsFields.IndexId)
                    .Add<int>(IndexingWorkStatsFields.IndexingAttempts)
                    .Add<int>(IndexingWorkStatsFields.IndexingSuccesses)
                    .Add<int>(IndexingWorkStatsFields.IndexingErrors)
                    .Add<long>(IndexingWorkStatsFields.LastIndexingTime)
                    .Add<long>(IndexingWorkStatsFields.CreatedTimestamp),
                bufferPool);

            IndexingMetadata = new Table(Tables.IndexingMetadata.TableName, bufferPool);
            LastIndexedEtags = new TableOfStructures<LastIndexedStatsFields>(Tables.LastIndexedEtags.TableName,
                new StructureSchema<LastIndexedStatsFields>()
                    .Add<int>(LastIndexedStatsFields.IndexId)
                    .Add<long>(LastIndexedStatsFields.LastTimestamp)
                    .Add<byte[]>(LastIndexedStatsFields.LastEtag),
                bufferPool);

            DocumentReferences = new TableOfStructures<DocumentReferencesFields>(Tables.DocumentReferences.TableName,
                new StructureSchema<DocumentReferencesFields>()
                    .Add<int>(DocumentReferencesFields.IndexId)
                    .Add<string>(DocumentReferencesFields.Key)
                    .Add<string>(DocumentReferencesFields.Reference),
                bufferPool, Tables.DocumentReferences.Indices.ByRef, Tables.DocumentReferences.Indices.ByView, Tables.DocumentReferences.Indices.ByViewAndKey, Tables.DocumentReferences.Indices.ByKey);

            Queues = new Table(Tables.Queues.TableName, bufferPool, Tables.Queues.Indices.ByName, Tables.Queues.Indices.Data);
            Lists = new Table(Tables.Lists.TableName, bufferPool, Tables.Lists.Indices.ByName, Tables.Lists.Indices.ByNameAndKey);

            Tasks = new TableOfStructures<TaskFields>(Tables.Tasks.TableName,
                new StructureSchema<TaskFields>()
                    .Add<int>(TaskFields.IndexId)
                    .Add<long>(TaskFields.AddedAt)
                    .Add<byte[]>(TaskFields.TaskId)
                    .Add<string>(TaskFields.Type)
                    .Add<byte[]>(TaskFields.SerializedTask),
                bufferPool, Tables.Tasks.Indices.ByIndexAndType, Tables.Tasks.Indices.ByType, Tables.Tasks.Indices.ByIndex);

            ScheduledReductions = new TableOfStructures<ScheduledReductionFields>(Tables.ScheduledReductions.TableName,
                new StructureSchema<ScheduledReductionFields>()
                    .Add<int>(ScheduledReductionFields.IndexId)
                    .Add<int>(ScheduledReductionFields.Bucket)
                    .Add<int>(ScheduledReductionFields.Level)
                    .Add<long>(ScheduledReductionFields.Timestamp)
                    .Add<string>(ScheduledReductionFields.ReduceKey)
                    .Add<byte[]>(ScheduledReductionFields.Etag),
                bufferPool, Tables.ScheduledReductions.Indices.ByView, Tables.ScheduledReductions.Indices.ByViewAndLevelAndReduceKey);
            
            MappedResults = new TableOfStructures<MappedResultFields>(Tables.MappedResults.TableName,
                new StructureSchema<MappedResultFields>()
                    .Add<int>(MappedResultFields.IndexId)
                    .Add<int>(MappedResultFields.Bucket)
                    .Add<long>(MappedResultFields.Timestamp)
                    .Add<string>(MappedResultFields.ReduceKey)
                    .Add<string>(MappedResultFields.DocId)
                    .Add<byte[]>(MappedResultFields.Etag),
                bufferPool, Tables.MappedResults.Indices.ByView, Tables.MappedResults.Indices.ByViewAndDocumentId, Tables.MappedResults.Indices.ByViewAndReduceKey, Tables.MappedResults.Indices.ByViewAndReduceKeyAndSourceBucket, Tables.MappedResults.Indices.Data);

            ReduceKeyCounts = new TableOfStructures<ReduceKeyCountFields>(Tables.ReduceKeyCounts.TableName,
                new StructureSchema<ReduceKeyCountFields>()
                    .Add<int>(ReduceKeyCountFields.IndexId)
                    .Add<int>(ReduceKeyCountFields.MappedItemsCount)
                    .Add<string>(ReduceKeyCountFields.ReduceKey),
                bufferPool, Tables.ReduceKeyCounts.Indices.ByView);

            ReduceKeyTypes = new TableOfStructures<ReduceKeyTypeFields>(Tables.ReduceKeyTypes.TableName,
                new StructureSchema<ReduceKeyTypeFields>()
                    .Add<int>(ReduceKeyTypeFields.IndexId)
                    .Add<int>(ReduceKeyTypeFields.ReduceType)
                    .Add<string>(ReduceKeyTypeFields.ReduceKey),
                bufferPool, Tables.ReduceKeyTypes.Indices.ByView);

            Attachments = new Table(Tables.Attachments.TableName, bufferPool, Tables.Attachments.Indices.ByEtag, Tables.Attachments.Indices.Metadata);

            ReduceResults = new TableOfStructures<ReduceResultFields>(Tables.ReduceResults.TableName,
                new StructureSchema<ReduceResultFields>()
                    .Add<int>(ReduceResultFields.IndexId)
                    .Add<int>(ReduceResultFields.Level)
                    .Add<int>(ReduceResultFields.SourceBucket)
                    .Add<int>(ReduceResultFields.Bucket)
                    .Add<long>(ReduceResultFields.Timestamp)
                    .Add<string>(ReduceResultFields.ReduceKey)
                    .Add<byte[]>(ReduceResultFields.Etag),
                bufferPool, Tables.ReduceResults.Indices.ByView, Tables.ReduceResults.Indices.ByViewAndReduceKeyAndLevel, Tables.ReduceResults.Indices.ByViewAndReduceKeyAndLevelAndSourceBucket, Tables.ReduceResults.Indices.ByViewAndReduceKeyAndLevelAndBucket, Tables.ReduceResults.Indices.Data);

            General = new Table(Tables.General.TableName, bufferPool);
            ReduceStats = new TableOfStructures<ReducingWorkStatsFields>(Tables.ReduceStats.TableName,
                new StructureSchema<ReducingWorkStatsFields>()
                    .Add<int>(ReducingWorkStatsFields.ReduceAttempts)
                    .Add<int>(ReducingWorkStatsFields.ReduceSuccesses)
                    .Add<int>(ReducingWorkStatsFields.ReduceErrors)
                    .Add<long>(ReducingWorkStatsFields.LastReducedTimestamp)
                    .Add<byte[]>(ReducingWorkStatsFields.LastReducedEtag),
                bufferPool);
        }

        private static string FindGraphviz()
        {
            var path = @"C:\Program Files (x86)\Graphviz2.";
            for (var i = 0; i < 100; i++)
            {
                var p = path + i.ToString("00");

                if (Directory.Exists(p))
                    return p;
            }

            throw new InvalidOperationException("No Graphviz found.");
        }

        public void SetDatabaseIdAndSchemaVersion(Guid id, string schemaVersion)
        {
            Id = id;
            SchemaVersion = schemaVersion;
        }

        public string SchemaVersion { get; private set; }

        public Guid Id { get; private set; }
    }
}
