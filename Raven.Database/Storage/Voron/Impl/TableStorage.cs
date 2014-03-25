// -----------------------------------------------------------------------
//  <copyright file="TableStorage.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Abstractions.Util.Streams;
using Raven.Database.Util.Streams;

using Voron.Impl.Backup;

namespace Raven.Database.Storage.Voron.Impl
{
	using System;
	using System.Collections.Generic;
	using System.Diagnostics;
	using System.IO;

	using global::Voron;
	using global::Voron.Debugging;
	using global::Voron.Impl;

	public class TableStorage : IDisposable
	{
	    private readonly StorageEnvironmentOptions _options;
	    private readonly IBufferPool _bufferPool;

	    private readonly StorageEnvironment _env;

		public TableStorage(StorageEnvironmentOptions options, IBufferPool bufferPool)
		{
			if (options == null)
				throw new ArgumentNullException("options");

		    _options = options;
		    _bufferPool = bufferPool;

			_env = new StorageEnvironment(options);

			Initialize();
			CreateSchema();
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
			return _env.CreateSnapshot();
		}

		public Table Details { get; private set; }

		public Table Documents { get; private set; }

		public Table IndexingStats { get; private set; }

		public Table ReduceStats { get; private set; }

		public Table LastIndexedEtags { get; private set; }

		public Table DocumentReferences { get; private set; }

		public Table Queues { get; private set; }

		public Table Lists { get; private set; }

		public Table Tasks { get; private set; }

		public Table ScheduledReductions { get; private set; }

		public Table MappedResults { get; private set; }

		public Table ReduceResults { get; private set; }

		public Table Attachments { get; private set; }

		public Table ReduceKeyCounts { get; private set; }

		public Table ReduceKeyTypes { get; private set; }

		public Table General { get; private set; }

		public StorageEnvironment Environment
		{
			get
			{
				return _env;
			}
		}

		public void Write(WriteBatch writeBatch)
		{
		    try
		    {
                _env.Writer.Write(writeBatch);
		    }
		    catch (AggregateException ae)
		    {
		        if (ae.InnerException is OperationCanceledException == false) // this can happen during storage disposal
		            throw;
		    }
		}

		public long GetEntriesCount(TableBase table)
		{
			using (var tx = _env.NewTransaction(TransactionFlags.Read))
			{
				return tx.State.GetTree(tx,table.TableName).State.EntriesCount;
			}
		}

		public void RenderAndShow(TableBase table, int showEntries = 25)
		{
			if (Debugger.IsAttached == false)
				return;

			using (var tx = _env.NewTransaction(TransactionFlags.Read))
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
			var p = Process.Start(@"c:\Program Files (x86)\Graphviz2.32\bin\dot.exe", "-Tsvg  " + path + " -o " + output);
			p.WaitForExit();
			Process.Start(output);
		}

		public void Dispose()
		{			if (_env != null)
				_env.Dispose();
		}

		//create all relevant storage trees in one place
		private void CreateSchema()
		{
			using (var tx = _env.NewTransaction(TransactionFlags.ReadWrite))
			{
				CreateDetailsSchema(tx);
				CreateDocumentsSchema(tx);
				CreateIndexingStatsSchema(tx);
				CreateLastIndexedEtagsSchema(tx);
				CreateDocumentReferencesSchema(tx);
				CreateQueuesSchema(tx);
				CreateListsSchema(tx);
				CreateTasksSchema(tx);
				CreateStalenessSchema(tx);
				CreateScheduledReductionsSchema(tx);
				CreateMappedResultsSchema(tx);
				CreateAttachmentsSchema(tx);
				CreateReduceKeyCountsSchema(tx);
				CreateReduceKeyTypesSchema(tx);
				CreateReduceResultsSchema(tx);
				CreateGeneralSchema(tx);
				CreateReduceStatsSchema(tx);

				tx.Commit();
			}
		}

		private void CreateReduceStatsSchema(Transaction tx)
		{
			_env.CreateTree(tx, Tables.ReduceStats.TableName);
		}

		private void CreateReduceResultsSchema(Transaction tx)
		{
			_env.CreateTree(tx, Tables.ReduceResults.TableName);
			_env.CreateTree(tx, ReduceResults.GetIndexKey(Tables.ReduceResults.Indices.ByViewAndReduceKeyAndLevel));
			_env.CreateTree(tx, ReduceResults.GetIndexKey(Tables.ReduceResults.Indices.ByViewAndReduceKeyAndLevelAndSourceBucket));
			_env.CreateTree(tx, ReduceResults.GetIndexKey(Tables.ReduceResults.Indices.ByViewAndReduceKeyAndLevelAndBucket));
			_env.CreateTree(tx, ReduceResults.GetIndexKey(Tables.ReduceResults.Indices.ByView));
			_env.CreateTree(tx, ReduceResults.GetIndexKey(Tables.ReduceResults.Indices.Data));
		}

		private void CreateReduceKeyCountsSchema(Transaction tx)
		{
			_env.CreateTree(tx, Tables.ReduceKeyCounts.TableName);
			_env.CreateTree(tx, ReduceKeyCounts.GetIndexKey(Tables.ReduceKeyCounts.Indices.ByView));
		}

		private void CreateReduceKeyTypesSchema(Transaction tx)
		{
			_env.CreateTree(tx, Tables.ReduceKeyTypes.TableName);
			_env.CreateTree(tx, ReduceKeyTypes.GetIndexKey(Tables.ReduceKeyCounts.Indices.ByView));
		}

		private void CreateAttachmentsSchema(Transaction tx)
		{
			_env.CreateTree(tx, Tables.Attachments.TableName);
			_env.CreateTree(tx, Attachments.GetIndexKey(Tables.Attachments.Indices.ByEtag));
            _env.CreateTree(tx, Attachments.GetIndexKey(Tables.Attachments.Indices.Metadata));
		}

		private void CreateMappedResultsSchema(Transaction tx)
		{
			_env.CreateTree(tx, Tables.MappedResults.TableName);
			_env.CreateTree(tx, MappedResults.GetIndexKey(Tables.MappedResults.Indices.ByView));
			_env.CreateTree(tx, MappedResults.GetIndexKey(Tables.MappedResults.Indices.ByViewAndDocumentId));
			_env.CreateTree(tx, MappedResults.GetIndexKey(Tables.MappedResults.Indices.ByViewAndReduceKey));
			_env.CreateTree(tx, MappedResults.GetIndexKey(Tables.MappedResults.Indices.ByViewAndReduceKeyAndSourceBucket));
			_env.CreateTree(tx, MappedResults.GetIndexKey(Tables.MappedResults.Indices.Data));
		}

		private void CreateScheduledReductionsSchema(Transaction tx)
		{
			_env.CreateTree(tx, Tables.ScheduledReductions.TableName);
			_env.CreateTree(tx, ScheduledReductions.GetIndexKey(Tables.ScheduledReductions.Indices.ByView));
			_env.CreateTree(tx, ScheduledReductions.GetIndexKey(Tables.ScheduledReductions.Indices.ByViewAndLevel));
			_env.CreateTree(tx, ScheduledReductions.GetIndexKey(Tables.ScheduledReductions.Indices.ByViewAndLevelAndReduceKey));
		}

		private void CreateStalenessSchema(Transaction tx)
		{
		}

		private void CreateTasksSchema(Transaction tx)
		{
			_env.CreateTree(tx, Tables.Tasks.TableName);
			_env.CreateTree(tx, Tasks.GetIndexKey(Tables.Tasks.Indices.ByIndexAndType));
			_env.CreateTree(tx, Tasks.GetIndexKey(Tables.Tasks.Indices.ByType));
			_env.CreateTree(tx, Tasks.GetIndexKey(Tables.Tasks.Indices.ByIndex));
		}

		private void CreateListsSchema(Transaction tx)
		{
			_env.CreateTree(tx, Tables.Lists.TableName);
			_env.CreateTree(tx, Lists.GetIndexKey(Tables.Lists.Indices.ByName));
			_env.CreateTree(tx, Lists.GetIndexKey(Tables.Lists.Indices.ByNameAndKey));
		}

		private void CreateQueuesSchema(Transaction tx)
		{
			_env.CreateTree(tx, Tables.Queues.TableName);
			_env.CreateTree(tx, Queues.GetIndexKey(Tables.Queues.Indices.ByName));
			_env.CreateTree(tx, Queues.GetIndexKey(Tables.Queues.Indices.Data));
		}

		private void CreateDocumentReferencesSchema(Transaction tx)
		{
			_env.CreateTree(tx, Tables.DocumentReferences.TableName);
			_env.CreateTree(tx, DocumentReferences.GetIndexKey(Tables.DocumentReferences.Indices.ByRef));
			_env.CreateTree(tx, DocumentReferences.GetIndexKey(Tables.DocumentReferences.Indices.ByView));
			_env.CreateTree(tx, DocumentReferences.GetIndexKey(Tables.DocumentReferences.Indices.ByViewAndKey));
			_env.CreateTree(tx, DocumentReferences.GetIndexKey(Tables.DocumentReferences.Indices.ByKey));
		}

		private void CreateLastIndexedEtagsSchema(Transaction tx)
		{
			_env.CreateTree(tx, Tables.LastIndexedEtags.TableName);
		}

		private void CreateIndexingStatsSchema(Transaction tx)
		{
			_env.CreateTree(tx, Tables.IndexingStats.TableName);
		}

		private void CreateDetailsSchema(Transaction tx)
		{
			_env.CreateTree(tx, Tables.Details.TableName);
		}

		private void CreateDocumentsSchema(Transaction tx)
		{
			_env.CreateTree(tx, Tables.Documents.TableName);
			_env.CreateTree(tx, Documents.GetIndexKey(Tables.Documents.Indices.KeyByEtag));
			_env.CreateTree(tx, Documents.GetIndexKey(Tables.Documents.Indices.Metadata));
		}

		private void CreateGeneralSchema(Transaction tx)
		{
			_env.CreateTree(tx, Tables.General.TableName);
		}

		private void Initialize()
		{
			Documents = new Table(Tables.Documents.TableName, _bufferPool, Tables.Documents.Indices.KeyByEtag, Tables.Documents.Indices.Metadata);
			Details = new Table(Tables.Details.TableName, _bufferPool);
            IndexingStats = new Table(Tables.IndexingStats.TableName, _bufferPool);
            LastIndexedEtags = new Table(Tables.LastIndexedEtags.TableName, _bufferPool);
            DocumentReferences = new Table(Tables.DocumentReferences.TableName, _bufferPool, Tables.DocumentReferences.Indices.ByRef, Tables.DocumentReferences.Indices.ByView, Tables.DocumentReferences.Indices.ByViewAndKey, Tables.DocumentReferences.Indices.ByKey);
            Queues = new Table(Tables.Queues.TableName, _bufferPool, Tables.Queues.Indices.ByName, Tables.Queues.Indices.Data);
            Lists = new Table(Tables.Lists.TableName, _bufferPool, Tables.Lists.Indices.ByName, Tables.Lists.Indices.ByNameAndKey);
            Tasks = new Table(Tables.Tasks.TableName, _bufferPool, Tables.Tasks.Indices.ByIndexAndType, Tables.Tasks.Indices.ByType, Tables.Tasks.Indices.ByIndex);
            ScheduledReductions = new Table(Tables.ScheduledReductions.TableName, _bufferPool, Tables.ScheduledReductions.Indices.ByView, Tables.ScheduledReductions.Indices.ByViewAndLevelAndReduceKey, Tables.ScheduledReductions.Indices.ByViewAndLevel);
            MappedResults = new Table(Tables.MappedResults.TableName, _bufferPool, Tables.MappedResults.Indices.ByView, Tables.MappedResults.Indices.ByViewAndDocumentId, Tables.MappedResults.Indices.ByViewAndReduceKey, Tables.MappedResults.Indices.ByViewAndReduceKeyAndSourceBucket, Tables.MappedResults.Indices.Data);
            ReduceKeyCounts = new Table(Tables.ReduceKeyCounts.TableName, _bufferPool, Tables.ReduceKeyCounts.Indices.ByView);
            ReduceKeyTypes = new Table(Tables.ReduceKeyTypes.TableName, _bufferPool, Tables.ReduceKeyTypes.Indices.ByView);
            Attachments = new Table(Tables.Attachments.TableName, _bufferPool, Tables.Attachments.Indices.ByEtag, Tables.Attachments.Indices.Metadata);
            ReduceResults = new Table(Tables.ReduceResults.TableName, _bufferPool, Tables.ReduceResults.Indices.ByView, Tables.ReduceResults.Indices.ByViewAndReduceKeyAndLevel, Tables.ReduceResults.Indices.ByViewAndReduceKeyAndLevelAndSourceBucket, Tables.ReduceResults.Indices.ByViewAndReduceKeyAndLevelAndBucket, Tables.ReduceResults.Indices.Data);
            General = new Table(Tables.General.TableName, _bufferPool);
            ReduceStats = new Table(Tables.ReduceStats.TableName, _bufferPool);
		}

	}
}