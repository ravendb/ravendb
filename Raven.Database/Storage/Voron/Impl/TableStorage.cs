// -----------------------------------------------------------------------
//  <copyright file="TableStorage.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Threading;
using Raven.Abstractions.Util.Streams;
using Raven.Database.Indexing.Collation.Cultures;

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

		internal Dictionary<string, object> GenerateReportOnStorage()
		{
			var reportData = new Dictionary<string, object>
	        {
	            {"MaxNodeSize", _options.DataPager.MaxNodeSize},
	            {"NumberOfAllocatedPages", _options.DataPager.NumberOfAllocatedPages},
	           // {"PageMaxSpace", persistenceSource.Options.DataPager.PageMaxSpace},
	            {"PageMinSpace", _options.DataPager.PageMinSpace},
	           // {"PageSize", persistenceSource.Options.DataPager.PageSize},
                {"Documents", GetEntriesCount(Documents)},
                {"Indexes", GetEntriesCount(IndexingStats)},
                {"Attachments", GetEntriesCount(Attachments)},

	        };

			return reportData;
		}

		public SnapshotReader CreateSnapshot()
		{
			return env.CreateSnapshot();
		}

		public Table Details { get; private set; }

		public Table Documents { get; private set; }

		public Table IndexingStats { get; private set; }

		public Table ReduceStats { get; private set; }

		public Table IndexingMetadata { get; private set; }

		public Table LastIndexedEtags { get; private set; }

		public Table DocumentReferences { get; private set; }

		public Table Queues { get; private set; }

		public Table Lists { get; private set; }

		public Table Tasks { get; private set; }

		public Table ScheduledReductions { get; private set; }

		public Table MappedResults { get; private set; }

		public Table ReduceResults { get; private set; }

        [Obsolete("Use RavenFS instead.")]
		public Table Attachments { get; private set; }

		public Table ReduceKeyCounts { get; private set; }

		public Table ReduceKeyTypes { get; private set; }

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
				return tx.State.GetTree(tx,table.TableName).State.EntriesCount;
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

			var tree = tx.State.GetTree(tx, table.TableName);

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
			IndexingStats = new Table(Tables.IndexingStats.TableName, bufferPool);
			IndexingMetadata = new Table(Tables.IndexingMetadata.TableName, bufferPool);
			LastIndexedEtags = new Table(Tables.LastIndexedEtags.TableName, bufferPool);
			DocumentReferences = new Table(Tables.DocumentReferences.TableName, bufferPool, Tables.DocumentReferences.Indices.ByRef, Tables.DocumentReferences.Indices.ByView, Tables.DocumentReferences.Indices.ByViewAndKey, Tables.DocumentReferences.Indices.ByKey);
			Queues = new Table(Tables.Queues.TableName, bufferPool, Tables.Queues.Indices.ByName, Tables.Queues.Indices.Data);
			Lists = new Table(Tables.Lists.TableName, bufferPool, Tables.Lists.Indices.ByName, Tables.Lists.Indices.ByNameAndKey);
			Tasks = new Table(Tables.Tasks.TableName, bufferPool, Tables.Tasks.Indices.ByIndexAndType, Tables.Tasks.Indices.ByType, Tables.Tasks.Indices.ByIndex);
			ScheduledReductions = new Table(Tables.ScheduledReductions.TableName, bufferPool, Tables.ScheduledReductions.Indices.ByView, Tables.ScheduledReductions.Indices.ByViewAndLevelAndReduceKey);
			MappedResults = new Table(Tables.MappedResults.TableName, bufferPool, Tables.MappedResults.Indices.ByView, Tables.MappedResults.Indices.ByViewAndDocumentId, Tables.MappedResults.Indices.ByViewAndReduceKey, Tables.MappedResults.Indices.ByViewAndReduceKeyAndSourceBucket, Tables.MappedResults.Indices.Data);
			ReduceKeyCounts = new Table(Tables.ReduceKeyCounts.TableName, bufferPool, Tables.ReduceKeyCounts.Indices.ByView);
			ReduceKeyTypes = new Table(Tables.ReduceKeyTypes.TableName, bufferPool, Tables.ReduceKeyTypes.Indices.ByView);
			Attachments = new Table(Tables.Attachments.TableName, bufferPool, Tables.Attachments.Indices.ByEtag, Tables.Attachments.Indices.Metadata);
			ReduceResults = new Table(Tables.ReduceResults.TableName, bufferPool, Tables.ReduceResults.Indices.ByView, Tables.ReduceResults.Indices.ByViewAndReduceKeyAndLevel, Tables.ReduceResults.Indices.ByViewAndReduceKeyAndLevelAndSourceBucket, Tables.ReduceResults.Indices.ByViewAndReduceKeyAndLevelAndBucket, Tables.ReduceResults.Indices.Data);
			General = new Table(Tables.General.TableName, bufferPool);
			ReduceStats = new Table(Tables.ReduceStats.TableName, bufferPool);
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